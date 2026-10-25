"""Weekly ATS and OTC files from the old OTC Transparency site (2014-2018).

The old site (otctransparency.finra.org grids, scraped by the original
``retrieve.py``) is gone, and the API's weekly history starts in late 2021, so
these files are the only copy of that history. ``import_legacy`` copies them into
the raw store as-is. Their names record the week, category, report type and
when FINRA last updated them:

    data/<year>/2014-06-23_ATS_NMS Tier 1_2015-10-07 00:00:00.csv
"""

import datetime as dt
import io
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv

from .raw import Capture, RawStore

SOURCE = "legacy/weekly"

CATEGORY = "Category"
REPORT_TYPE = "Report_Type"
SYMBOL = "Symbol"
WEEK = "Week"
VENUE_DESCRIPTION = "Venue_Description"

# ATS files name the venue ATS_Description; OTC files, OTC_Non-ATS_Description.
VENUE_COLUMNS = ["ATS_Description", "OTC_Non-ATS_Description"]

FIELDS: list[pa.Field[Any]] = [
    pa.field(WEEK, pa.date32(), nullable=False),
    pa.field(CATEGORY, pa.string(), nullable=False),
    pa.field(REPORT_TYPE, pa.string()),
    pa.field(SYMBOL, pa.string()),
    pa.field(VENUE_DESCRIPTION, pa.string()),
    pa.field("Issue_Description", pa.string()),
    pa.field("ATS_MPID", pa.string()),  # ATS files only
    pa.field("Shares", pa.int64()),
    pa.field("Shares_Last_Updated", pa.date32()),
    pa.field("Trades", pa.int64()),
    pa.field("Trades_Last_Updated", pa.date32()),
]
SCHEMA = pa.schema(FIELDS)

# Older files write dates as 28-JUL-14.
DATE_FORMATS: list[Any] = [pv.ISO8601, "%d-%b-%y"]

_NAME = re.compile(
    r"(\d{4}-\d{2}-\d{2})_(ATS|OTC)_(.+)_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
)


@dataclass
class ImportResult:
    saved: int = 0
    already: int = 0
    ignored: list[str] = field(default_factory=list)  # names not understood


@dataclass(frozen=True)
class LegacyFile:
    week: dt.date
    category: str
    report_type: str
    updated: dt.datetime

    @property
    def key(self) -> str:
        report = re.sub(r"[^A-Za-z0-9]+", "-", self.report_type).strip("-")
        updated = f"{self.updated:%Y%m%dT%H%M%S}"
        return f"{self.week:%Y-%m-%d}_{self.category}_{report}_{updated}"

    @property
    def group(self) -> tuple[dt.date, str, str]:
        return self.week, self.category, self.report_type


def parse_filename(stem: str) -> LegacyFile:
    """Parse ``2014-06-23_ATS_NMS Tier 1_2015-10-07 00:00:00``."""
    m = _NAME.fullmatch(stem)
    if m is None:
        raise ValueError(f"not a legacy weekly file name: {stem!r}")
    week, category, report_type, updated = m.groups()
    return LegacyFile(
        dt.date.fromisoformat(week),
        category,
        report_type,
        dt.datetime.fromisoformat(updated),
    )


def legacy_files(folder: Path) -> Iterator[Path]:
    for d in sorted(folder.iterdir()):
        if d.is_dir() and d.name.isdigit():
            yield from sorted(d.glob("*.csv"))


def import_legacy(
    store: RawStore,
    folder: Path,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    progress: Callable[[str], None] = lambda _: None,
) -> ImportResult:
    """Copy legacy files into the raw store."""
    latest = store.latest(SOURCE)
    result = ImportResult()
    for path in legacy_files(folder):
        try:
            info = parse_filename(path.stem)
        except ValueError:
            result.ignored.append(str(path))
            continue
        content = path.read_bytes()
        previous = latest.get(info.key)
        if previous is not None and _intact_copy(store, previous, content):
            result.already += 1
            continue
        meta = {
            "original_name": path.name,
            "week": info.week.isoformat(),
            "category": info.category,
            "report_type": info.report_type,
            "last_updated": info.updated.isoformat(),
        }
        files: list[tuple[str, bytes, dict[str, Any]]] = [
            (f"{info.key}.csv", content, {})
        ]
        store.save(SOURCE, info.key, files, meta=meta, now=now())
        result.saved += 1
        progress(path.name)
    return result


def _intact_copy(store: RawStore, previous: Capture, content: bytes) -> bool:
    if previous.sha256s != [Capture.sha_of(content)]:
        return False
    try:
        store.read(previous.files[0])
    except OSError:
        return False  # damaged: import it again
    return True


def latest_versions(captures: list[Capture]) -> list[Capture]:
    """The newest file for each (week, category, report type), oldest week first."""
    latest: dict[tuple[dt.date, str, str], tuple[dt.datetime, Capture]] = {}
    for capture in captures:
        info = parse_filename(Path(capture.meta["original_name"]).stem)
        if info.group not in latest or info.updated > latest[info.group][0]:
            latest[info.group] = (info.updated, capture)
    return [latest[group][1] for group in sorted(latest)]


def read_table(content: bytes, week: dt.date, category: str) -> pa.Table:
    """One weekly file as a table with ``SCHEMA``."""
    footer: list[int] = []

    def skip_footer(row: Any) -> Literal["skip", "error"]:
        # Each file ends with a bare row count.
        if row.actual_columns == 1 and row.text.strip().isdigit():
            footer.append(int(row.text.strip()))
            return "skip"
        return "error"

    types: dict[str, pa.DataType] = {f.name: f.type for f in SCHEMA}
    for name in VENUE_COLUMNS:
        types[name] = pa.string()
    for name in ("Shares_Last_Updated", "Trades_Last_Updated"):
        types[name] = pa.timestamp("s")
    table = pv.read_csv(
        io.BytesIO(content),
        parse_options=pv.ParseOptions(
            delimiter="|", newlines_in_values=True, invalid_row_handler=skip_footer
        ),
        convert_options=pv.ConvertOptions(
            column_types=types,
            timestamp_parsers=DATE_FORMATS,
            null_values=[""],
            strings_can_be_null=True,
        ),
    )
    # Every file ends with a row count; one without was cut short.
    if footer != [table.num_rows]:
        raise ValueError(f"row-count footer {footer} for {table.num_rows} rows")
    table = table.rename_columns(
        [VENUE_DESCRIPTION if c in VENUE_COLUMNS else c for c in table.column_names]
    )
    unknown = set(table.column_names) - set(SCHEMA.names)
    if unknown:
        raise ValueError(f"unexpected columns {sorted(unknown)}")
    n = table.num_rows
    columns: list[Any] = []
    for f in SCHEMA:
        if f.name == WEEK:
            columns.append(pa.array([week] * n, pa.date32()))
        elif f.name == CATEGORY:
            columns.append(pa.array([category] * n, pa.string()))
        elif f.name in table.column_names and pa.types.is_date32(f.type):
            # Parsed as timestamps; a time of day would be lost as a date.
            ts = table[f.name]
            days = ts.cast(pa.date32())
            if pc.all(pc.equal(days.cast(ts.type), ts)).as_py() is False:
                raise ValueError(f"{f.name}: a date has a time of day")
            columns.append(days)
        elif f.name in table.column_names:
            columns.append(table[f.name].cast(f.type))
        else:
            columns.append(pa.nulls(n, f.type))
    return pa.Table.from_arrays(columns, schema=SCHEMA)
