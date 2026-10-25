"""Build Parquet tables from the raw store.

    parquet/api/<dataset>/<partition>.parquet          one file per API partition
    parquet/cdn/regsho_daily/<facility>/<YYYY-MM>.parquet
    parquet/cdn/regsho_monthly/<facility>sh<YYYYMM>[_<part>].parquet
    parquet/legacy/weekly/<YYYY>.parquet

Each output is built from the newest capture of each of its inputs, and is
rebuilt only when those inputs or the code that builds them change: a
fingerprint of both is stored in the file's metadata. Files are written under a
hidden temporary name (which dataset readers skip) and then renamed. When an
output can't be built, its old file is removed, so readers never see a table
that no longer matches the raw store. Read a table with, for example,
``pyarrow.parquet.read_table("data/parquet/api/weeklySummary")``.

Every file of an API dataset has the same columns: each field in any saved
version of the dataset's metadata. Types follow the metadata: String to
string, Date to date32 (timestamp for the OTC daily list, whose dates carry
times), and Number to float64, or to int64 for counts, ranks, IDs and CRD
numbers. Share quantities stay float64 because FINRA reports fractional shares.
Values are checked as they are converted: a fractional count, a date with a
time of day, an infinite or NaN number, or text in a number column fails the
build of that file instead of being altered.
"""

import csv
import hashlib
import io
import json
import zipfile
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date
from functools import partial
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq

from . import api, cdn, legacy
from .raw import Capture, RawStore

# pyarrow-stubs types these functions too narrowly for string arrays.
_pc: Any = pc

# Outputs are rebuilt when the code that builds them changes.
CODE = hashlib.sha256(
    Path(__file__).read_bytes() + Path(legacy.__file__).read_bytes()
).hexdigest()
FINGERPRINT = b"finra_otc.fingerprint"
INPUTS = b"finra_otc.inputs"

# The OTC daily list's Date fields carry times (an exDate of 11:06:49), all but
# calendarDay, its partition.
TIMESTAMP_DATASETS = {"otcDailyList": frozenset({"calendarDay"})}
INTEGER_SUFFIXES = ("Count", "Rank", "ID", "CRDNumber", "RoundLotQuantity")
INTEGER_FIELDS = frozenset({"accountingYearMonthNumber"})

# Errors that mean one output can't be built; the others still are.
BUILD_ERRORS = (
    pa.ArrowInvalid,
    OSError,  # includes raw.CorruptFile
    EOFError,
    ValueError,  # includes UnicodeDecodeError
    zipfile.BadZipFile,
    zlib.error,
    NotImplementedError,  # a ZIP compression method zipfile lacks
)


class BuildError(ValueError):
    pass


@dataclass
class Result:
    source: str
    built: int = 0
    current: int = 0
    rows: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def parquet_dir(store: RawStore) -> Path:
    return store.root / "parquet"


def fingerprint(inputs: Iterable[Capture], extra: Any = None) -> str:
    payload = {
        "code": CODE,
        "inputs": [[c.source, c.key, c.captured_at, c.sha256s] for c in inputs],
        "extra": extra,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def is_current(path: Path, print_: str) -> bool:
    if not path.exists():
        return False
    try:
        metadata = pq.read_schema(path).metadata or {}
    except OSError, pa.ArrowInvalid:
        return False
    return metadata.get(FINGERPRINT) == print_.encode()


def _with_metadata(schema: pa.Schema, print_: str, inputs: list[Capture]) -> pa.Schema:
    described = [
        {"source": c.source, "key": c.key, "captured_at": c.captured_at} for c in inputs
    ]
    return schema.with_metadata(
        {FINGERPRINT: print_.encode(), INPUTS: json.dumps(described).encode()}
    )


def temporary(path: Path) -> Path:
    # Dataset readers skip names starting with "." or "_".
    return path.with_name(f".{path.name}.tmp")


def write_table(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = temporary(path)
    try:
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def build_one(
    result: Result,
    path: Path,
    print_: str,
    label: str,
    write: Callable[[], int],
    progress: Callable[[str], None],
) -> None:
    """Build one output unless current; ``write`` writes it and returns rows."""
    if is_current(path, print_):
        result.current += 1
        return
    try:
        rows = write()
    except BUILD_ERRORS as e:
        path.unlink(missing_ok=True)
        result.failed += 1
        result.errors.append(f"{label}: {e}")
        return
    result.built += 1
    result.rows += rows
    progress(f"{label}: {rows:,} rows")


def header_names(first_line: bytes, delimiter: str, quoted: bool) -> list[str]:
    text = first_line.decode("utf-8").rstrip("\r\n")
    if quoted:
        return next(csv.reader([text], delimiter=delimiter))
    return text.split(delimiter)


def string_options(names: list[str]) -> pv.ConvertOptions:
    """Every column read as a string, with empty values as null.

    The API leaves nulls unquoted, except weeklySummaryHistoric, which quotes
    them as "". Both are read as null, so the Parquet tables agree; the raw
    files keep the difference.
    """
    return pv.ConvertOptions(
        null_values=[""],
        strings_can_be_null=True,
        quoted_strings_can_be_null=True,
        column_types={name: pa.string() for name in names},
    )


def _read_strings(content: bytes, delimiter: str, quoted: bool) -> pa.Table:
    names = header_names(content.split(b"\n", 1)[0], delimiter, quoted)
    return pv.read_csv(
        io.BytesIO(content),
        # Quoted values may hold line breaks.
        parse_options=pv.ParseOptions(
            delimiter=delimiter,
            quote_char='"' if quoted else False,
            newlines_in_values=quoted,
        ),
        convert_options=string_options(names),
    )


def api_type(dataset: str, name: str, finra_type: str) -> pa.DataType:
    if finra_type == "String":
        return pa.string()
    if finra_type == "Date":
        dates = TIMESTAMP_DATASETS.get(dataset)
        if dates is not None and name not in dates:
            return pa.timestamp("ms")
        return pa.date32()
    if finra_type == "Number":
        if name in INTEGER_FIELDS or name.endswith(INTEGER_SUFFIXES):
            return pa.int64()
        return pa.float64()
    raise BuildError(f"unknown FINRA type {finra_type!r} for {name}")


def api_schema(dataset: str, metadata: dict[str, Any]) -> pa.Schema:
    return pa.schema(
        [
            pa.field(f["name"], api_type(dataset, f["name"], f["type"]))
            for f in metadata["fields"]
        ]
    )


def to_date(ts: Any, name: str) -> Any:
    """Timestamps to dates, refusing any with a time of day."""
    days = ts.cast(pa.date32())
    # Casting a timestamp to a date silently drops the time of day.
    if pc.all(pc.equal(days.cast(ts.type), ts)).as_py() is False:
        raise BuildError(f"{name}: a date has a time of day")
    return days


def convert(
    column: pa.ChunkedArray[Any] | pa.Array[Any], to: pa.DataType, name: str
) -> Any:
    """Convert a string column exactly, or raise."""
    try:
        if pa.types.is_string(to):
            return column
        if pa.types.is_int64(to):
            return column.cast(pa.decimal128(38, 9)).cast(pa.int64())
        if pa.types.is_floating(to):
            values = column.cast(to)
            # Overflow (1e400) parses as infinity; FINRA never sends NaN.
            if pc.all(pc.is_finite(values)).as_py() is False:
                raise BuildError(f"{name}: a number is infinite or NaN")
            return values
        if pa.types.is_timestamp(to):
            return column.cast(to)
        if pa.types.is_date32(to):
            return to_date(column.cast(pa.timestamp("ms")), name)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
        raise BuildError(f"{name}: {e}") from e
    raise BuildError(f"{name}: no conversion to {to}")


def conform(table: pa.Table, schema: pa.Schema, what: str) -> pa.Table:
    """Strings to ``schema``: same columns (in any order), converted exactly."""
    unknown = set(table.column_names) - set(schema.names)
    if unknown:
        raise BuildError(f"{what}: columns not in the metadata: {sorted(unknown)}")
    columns = []
    for f in schema:
        if f.name in table.column_names:
            columns.append(convert(table[f.name], f.type, f"{what} {f.name}"))
        else:
            columns.append(pa.nulls(table.num_rows, f.type))
    return pa.Table.from_arrays(columns, schema=schema)


def build_api_partition(
    store: RawStore, capture: Capture, schema: pa.Schema
) -> pa.Table:
    tables = []
    for f in capture.files:
        table = _read_strings(store.read(f), ",", quoted=True)
        if table.num_rows != f.meta.get("rows"):
            raise BuildError(f"{f.path}: {table.num_rows} rows, manifest says {f.meta}")
        tables.append(conform(table, schema, f.path))
    table = pa.concat_tables(tables) if tables else schema.empty_table()
    total = capture.meta.get("record_total")
    if table.num_rows != total:
        raise BuildError(f"{capture.source} {capture.key}: {table.num_rows} != {total}")
    return table


def dataset_schema(store: RawStore, dataset: str) -> pa.Schema:
    """One schema for every file of a dataset: each field FINRA has ever listed.

    Each partition's file gets the same columns, null where its capture lacks
    them, so reading the dataset as a directory loses nothing. A field whose
    type changed between metadata versions fails the dataset.
    """
    found = sorted(
        (c for c in store.captures(f"api_catalog/{dataset}") if c.key == "metadata"),
        key=lambda c: c.captured_at,
    )
    types: dict[str, pa.DataType] = {}
    for capture in found:
        for f in api_schema(dataset, json.loads(store.read(capture.files[0]))):
            if f.name in types and types[f.name] != f.type:
                raise BuildError(
                    f"{f.name} changed type from {types[f.name]} to {f.type}"
                )
            types.setdefault(f.name, f.type)
    if not types:
        raise BuildError("no metadata captured")
    return pa.schema([pa.field(name, t) for name, t in types.items()])


def _remove_stale(path: Path, capture: Capture) -> None:
    """Remove an output that was built from an older capture than ``capture``."""
    if not path.exists():
        return
    try:
        inputs = json.loads((pq.read_schema(path).metadata or {}).get(INPUTS, b"[]"))
    except BUILD_ERRORS:
        inputs = []
    if [i.get("captured_at") for i in inputs] != [capture.captured_at]:
        path.unlink()


def build_api(
    store: RawStore,
    datasets: list[str] | None = None,
    progress: Callable[[str], None] = lambda _: None,
) -> list[Result]:
    results = []
    for dataset in datasets or [d.name for d in api.DATASETS]:
        source = f"api/{dataset}"
        result = Result(source)
        results.append(result)
        captures = store.latest(source)
        if not captures:
            continue
        out = parquet_dir(store) / "api" / dataset
        try:
            schema = dataset_schema(store, dataset)
        except BUILD_ERRORS as e:
            # Nothing can be built; drop outputs the raw store has moved past.
            for key, capture in captures.items():
                _remove_stale(out / f"{key}.parquet", capture)
            result.failed += 1
            result.errors.append(f"metadata: {e}")
            continue
        for key, capture in sorted(captures.items()):
            path = out / f"{key}.parquet"
            print_ = fingerprint([capture], schema.to_string())
            write = partial(_write_api, store, capture, schema, path, print_)
            build_one(result, path, print_, f"{source} {key}", write, progress)
    return results


def _write_api(
    store: RawStore, capture: Capture, schema: pa.Schema, path: Path, print_: str
) -> int:
    table = build_api_partition(store, capture, schema)
    metadata = _with_metadata(schema, print_, [capture]).metadata
    write_table(path, table.replace_schema_metadata(metadata))
    return table.num_rows


def _footer_handler(
    footer: list[str],
) -> Callable[[Any], Literal["skip", "error"]]:
    def handle(row: Any) -> Literal["skip", "error"]:
        # Reg SHO files end with a bare row count.
        if row.actual_columns == 1 and row.text.strip().isdigit():
            footer.append(row.text.strip())
            return "skip"
        return "error"

    return handle


def _check_footer(footer: list[str], rows: int, what: str) -> None:
    if len(footer) != 1 or int(footer[0]) != rows:
        raise BuildError(f"{what}: row-count footer {footer} for {rows} rows")


def _yyyymmdd(column: Any, what: str) -> Any:
    try:
        return _pc.strptime(column, "%Y%m%d", "s").cast(pa.date32())
    except pa.ArrowInvalid as e:
        raise BuildError(f"{what} Date: {e}") from e


DAILY_FIELDS: list[pa.Field[Any]] = [
    pa.field("Date", pa.date32(), nullable=False),
    pa.field("Symbol", pa.string(), nullable=False),
    pa.field("ShortVolume", pa.float64()),
    pa.field("ShortExemptVolume", pa.float64()),  # from 2011
    pa.field("TotalVolume", pa.float64()),
    pa.field("Market", pa.string()),
    pa.field("Facility", pa.string(), nullable=False),
]
DAILY_SCHEMA = pa.schema(DAILY_FIELDS)


def read_daily(content: bytes, facility: str, what: str) -> pa.Table:
    footer: list[str] = []
    names = header_names(content.split(b"\n", 1)[0], "|", quoted=False)
    table = pv.read_csv(
        io.BytesIO(content),
        parse_options=pv.ParseOptions(
            delimiter="|", quote_char=False, invalid_row_handler=_footer_handler(footer)
        ),
        convert_options=string_options(names),
    )
    _check_footer(footer, table.num_rows, what)
    unknown = set(table.column_names) - set(DAILY_SCHEMA.names)
    if unknown:
        raise BuildError(f"{what}: unexpected columns {sorted(unknown)}")
    n = table.num_rows
    columns = []
    for f in DAILY_SCHEMA:
        if f.name == "Facility":
            columns.append(pa.array([facility] * n, pa.string()))
        elif f.name == "Date":
            columns.append(_yyyymmdd(table["Date"], what))
        elif f.name in table.column_names:
            columns.append(convert(table[f.name], f.type, f"{what} {f.name}"))
        else:
            columns.append(pa.nulls(n, f.type))
    return pa.Table.from_arrays(columns, schema=DAILY_SCHEMA)


def build_daily(
    store: RawStore, progress: Callable[[str], None] = lambda _: None
) -> Result:
    result = Result(cdn.DAILY_SOURCE)
    groups: dict[tuple[str, str], list[Capture]] = defaultdict(list)
    for capture in store.latest(cdn.DAILY_SOURCE).values():
        groups[(capture.meta["facility"], capture.meta["date"][:7])].append(capture)
    for (facility, month), captures in sorted(groups.items()):
        captures.sort(key=lambda c: c.meta["date"])
        path = (
            parquet_dir(store) / "cdn" / "regsho_daily" / facility / f"{month}.parquet"
        )
        print_ = fingerprint(captures)
        write = partial(_write_daily, store, captures, facility, path, print_)
        label = f"regsho_daily {facility} {month}"
        build_one(result, path, print_, label, write, progress)
    return result


def _write_daily(
    store: RawStore, captures: list[Capture], facility: str, path: Path, print_: str
) -> int:
    tables = [read_daily(store.read(c.files[0]), facility, c.key) for c in captures]
    schema = _with_metadata(DAILY_SCHEMA, print_, captures)
    table = pa.concat_tables(tables).replace_schema_metadata(schema.metadata)
    write_table(path, table)
    return table.num_rows


MONTHLY_FIELDS: list[pa.Field[Any]] = [
    pa.field("MarketCenter", pa.string()),
    pa.field("Symbol", pa.string(), nullable=False),
    pa.field("Date", pa.date32(), nullable=False),
    pa.field("Time", pa.time32("ms")),  # Parquet stores no coarser unit
    pa.field("ShortType", pa.string()),
    pa.field("Size", pa.float64()),
    pa.field("Price", pa.float64()),
    # Blank when there's no link; old files pad it with a NUL byte or a space.
    pa.field("LinkIndicator", pa.string()),
]
MONTHLY_SCHEMA = pa.schema(MONTHLY_FIELDS)


def _blank_to_null(column: Any) -> Any:
    trimmed = _pc.utf8_trim(column, characters="\x00 ")
    return _pc.if_else(_pc.equal(trimmed, ""), pa.scalar(None, pa.string()), trimmed)


def _monthly_batches(
    stream: Any, names: list[str], footer: list[str], what: str
) -> Iterator[pa.RecordBatch]:
    reader = pv.open_csv(
        stream,
        read_options=pv.ReadOptions(block_size=64 << 20),
        parse_options=pv.ParseOptions(
            delimiter="|", quote_char=False, invalid_row_handler=_footer_handler(footer)
        ),
        convert_options=string_options(names),
    )
    names = reader.schema.names
    # Newer files end every line, header included, with a delimiter.
    extra = [n for n in names if n not in MONTHLY_SCHEMA.names]
    if extra not in ([], [""]):
        raise BuildError(f"{what}: unexpected columns {extra}")
    for batch in reader:
        if extra:
            trailing = batch.column(names.index(""))
            if trailing.null_count != len(trailing):
                raise BuildError(f"{what}: values after the last column")
        columns = []
        for f in MONTHLY_SCHEMA:
            if f.name not in names:
                columns.append(pa.nulls(batch.num_rows, f.type))
                continue
            col = batch.column(names.index(f.name))
            if f.name == "Date":
                columns.append(_yyyymmdd(col, what))
            elif f.name == "Time":
                try:
                    stamped = _pc.strptime(
                        _pc.binary_join_element_wise("19700101", col, " "),
                        "%Y%m%d %H:%M:%S",
                        "s",
                    )
                except pa.ArrowInvalid as e:
                    raise BuildError(f"{what} Time: {e}") from e
                columns.append(stamped.cast(pa.time32("ms")))
            elif f.name == "LinkIndicator":
                columns.append(_blank_to_null(col))
            else:
                columns.append(convert(col, f.type, f"{what} {f.name}"))
        yield pa.RecordBatch.from_arrays(columns, schema=MONTHLY_SCHEMA)


def build_monthly_file(
    store: RawStore, capture: Capture, path: Path, print_: str
) -> int:
    store.verify(capture.files[0])
    schema = _with_metadata(MONTHLY_SCHEMA, print_, [capture])
    footer: list[str] = []
    rows = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = temporary(path)
    try:
        with zipfile.ZipFile(store.path(capture.files[0])) as z:
            members = [m for m in z.namelist() if not m.endswith("/")]
            if len(members) != 1:
                raise BuildError(f"{capture.key}: expected one file, got {members}")
            with z.open(members[0]) as stream:
                names = header_names(stream.readline(), "|", quoted=False)
            with (
                z.open(members[0]) as stream,
                pq.ParquetWriter(tmp, schema, compression="zstd") as writer,
            ):
                for batch in _monthly_batches(stream, names, footer, capture.key):
                    writer.write_batch(batch)
                    rows += batch.num_rows
        _check_footer(footer, rows, capture.key)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return rows


def build_monthly(
    store: RawStore, progress: Callable[[str], None] = lambda _: None
) -> Result:
    result = Result(cdn.MONTHLY_SOURCE)
    for key, capture in sorted(store.latest(cdn.MONTHLY_SOURCE).items()):
        path = parquet_dir(store) / "cdn" / "regsho_monthly" / f"{key}.parquet"
        print_ = fingerprint([capture])
        write = partial(build_monthly_file, store, capture, path, print_)
        build_one(result, path, print_, f"regsho_monthly {key}", write, progress)
    return result


def build_legacy(
    store: RawStore, progress: Callable[[str], None] = lambda _: None
) -> Result:
    result = Result(legacy.SOURCE)
    by_year: dict[int, list[Capture]] = defaultdict(list)
    for capture in legacy.latest_versions(list(store.latest(legacy.SOURCE).values())):
        by_year[int(capture.meta["week"][:4])].append(capture)
    for year, captures in sorted(by_year.items()):
        path = parquet_dir(store) / "legacy" / "weekly" / f"{year}.parquet"
        print_ = fingerprint(captures)
        write = partial(_write_legacy, store, captures, path, print_)
        build_one(result, path, print_, f"legacy weekly {year}", write, progress)
    return result


def _write_legacy(
    store: RawStore, captures: list[Capture], path: Path, print_: str
) -> int:
    tables = [
        legacy.read_table(
            store.read(c.files[0]),
            date.fromisoformat(c.meta["week"]),
            c.meta["category"],
        )
        for c in captures
    ]
    schema = _with_metadata(legacy.SCHEMA, print_, captures)
    table = pa.concat_tables(tables).replace_schema_metadata(schema.metadata)
    write_table(path, table)
    return table.num_rows
