"""Combine the weekly FINRA CSV files into one Parquet file, with pyarrow.

When FINRA republishes a week, ``retrieve.py`` saves the new file next to the
old one. Only the newest file for each (week, category, report type) is kept.
"""

import argparse
import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from tqdm import tqdm

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
DATE_FORMATS = [pv.ISO8601, "%d-%b-%y"]


def year_string(v):
    try:
        int(v)
        return True
    except ValueError:
        return False


def parse_filename(path):
    """(week, category, report type, last updated) from
    2014-06-23_ATS_NMS Tier 1_2015-10-07 00:00:00.csv"""
    week, category, report_type, updated = path.stem.split("_")
    return (
        dt.date.fromisoformat(week),
        category,
        report_type,
        dt.datetime.fromisoformat(updated),
    )


def iterdata(path):
    for d in path.iterdir():
        if d.is_dir() and year_string(d.name):
            yield from d.glob("*.csv")


def latest_files(paths):
    """The newest file for each (week, category, report type), oldest week first."""
    latest = {}
    for path in paths:
        week, category, report_type, updated = parse_filename(path)
        key = (week, category, report_type)
        if key not in latest or updated > latest[key][0]:
            latest[key] = (updated, path)
    return [latest[key][1] for key in sorted(latest)]


def _skip_footer(row):
    # Each file ends with a bare row count.
    if row.actual_columns == 1 and row.text.strip().isdigit():
        return "skip"
    return "error"


def read_table(path):
    """One weekly file as a table with ``SCHEMA``."""
    types = {f.name: f.type for f in SCHEMA}
    for name in VENUE_COLUMNS:
        types[name] = pa.string()
    for name in ("Shares_Last_Updated", "Trades_Last_Updated"):
        types[name] = pa.timestamp("s")
    table = pv.read_csv(
        path,
        parse_options=pv.ParseOptions(delimiter="|", invalid_row_handler=_skip_footer),
        convert_options=pv.ConvertOptions(
            column_types=types,
            timestamp_parsers=DATE_FORMATS,
            strings_can_be_null=True,
        ),
    )
    table = table.rename_columns(
        [VENUE_DESCRIPTION if c in VENUE_COLUMNS else c for c in table.column_names]
    )
    week, category, _, _ = parse_filename(path)
    n = table.num_rows
    columns = []
    for field in SCHEMA:
        if field.name == WEEK:
            columns.append(pa.array([week] * n, pa.date32()))
        elif field.name == CATEGORY:
            columns.append(pa.array([category] * n, pa.string()))
        elif field.name in table.column_names:
            columns.append(table[field.name].cast(field.type))
        else:
            columns.append(pa.nulls(n, field.type))
    return pa.Table.from_arrays(columns, schema=SCHEMA)


def write_parquet(paths, out_path, progress=True):
    """Writes every file in ``paths`` to ``out_path``, one row group per file.

    Writes to a temporary file first, so a failed run leaves the old file.
    Returns the number of rows written.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    rows = 0
    with pq.ParquetWriter(tmp_path, SCHEMA, compression="zstd") as writer:
        for path in tqdm(paths, unit="file", disable=not progress):
            table = read_table(path)
            writer.write_table(table)
            rows += table.num_rows
    tmp_path.replace(out_path)
    return rows


def get_parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", type=Path, default=Path("data"), help="CSV folder")
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data/otc.parquet"),
        help="Parquet file to write",
    )
    return p


def main(parser_args=None):
    args = get_parser().parse_args(args=parser_args)
    paths = latest_files(iterdata(args.data))
    rows = write_parquet(paths, args.out)
    print(f"{rows:,} rows from {len(paths)} files to {args.out}")


if __name__ == "__main__":
    main()
