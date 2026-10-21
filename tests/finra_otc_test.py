import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import sqlalchemy as sa

import retrieve
import sql
import upsert

GRID = """
<table>
  <tr><th> Week </th><th>Report Type</th><th>Year</th><th>Last Updated</th>
      <th>File</th></tr>
  <tr><td>10/05/2026</td><td>T1</td><td>2026</td><td>10/14/2026 10:00</td>
      <td><a href="/files/{cat}/1">csv</a></td></tr>
  <tr><td>09/28/2026</td><td>T2</td><td>2026</td><td>10/07/2026 09:30</td>
      <td><a href="/files/{cat}/2">csv</a></td></tr>
</table>
"""

WEEKLY = """\
Symbol|Issue_Description|ATS_Description|ATS_MPID|Report_Type|Shares|\
Shares_Last_Updated|Trades|Trades_Last_Updated
AAPL|Apple Inc.|SOME ATS|ABCD|T1|1000|2026-10-14|10|2026-10-14
MSFT|Microsoft Corp.|SOME ATS|ABCD|T1|2000|2026-10-14|20|2026-10-14
Total|||||3000||30|
"""


class FakeSession:
    def get(self, url):
        cat = "OTC" if "/OTC" in url else "ATS"
        return SimpleNamespace(text=GRID.format(cat=cat))


def test_grid_url():
    base = "https://otctransparency.finra.org"
    assert retrieve.grid_url("ATS") == (
        base + "/TradingDetailFileArchive/TradingDetailFilesDownloadGrid"
    )
    assert retrieve.grid_url("OTC") == (
        base + "/OTCTradingDetailFileArchive/OTCTradingDetailFilesDownloadGrid"
    )


def test_read_grid_frame_reads_both_categories():
    df = retrieve.read_grid_frame(FakeSession())
    assert list(df[sql.CATEGORY]) == ["ATS", "ATS", "OTC", "OTC"]
    assert list(df[retrieve.DOWNLOAD]) == [
        "/files/ATS/1",
        "/files/ATS/2",
        "/files/OTC/1",
        "/files/OTC/2",
    ]
    assert df[sql.WEEK].iloc[0] == pd.Timestamp("2026-10-05")
    assert df[retrieve.LAST_UPDATED].iloc[1] == pd.Timestamp("2026-10-07 09:30")


def test_parser_reads_date():
    args = retrieve.get_parser().parse_args(["2026-10-01"])
    assert args.earliest_updated_date == dt.datetime(2026, 10, 1)


def test_year_string():
    assert upsert.year_string("2026")
    assert not upsert.year_string("tmp")


def test_iterdata_skips_non_year_folders(tmp_path):
    for folder, name in [("2025", "a.csv"), ("2026", "b.csv"), ("misc", "c.csv")]:
        (tmp_path / folder).mkdir()
        (tmp_path / folder / name).touch()
    assert sorted(p.name for p in upsert.iterdata(tmp_path)) == ["a.csv", "b.csv"]
    assert upsert.count_files(tmp_path) == 2


def test_read_frame_indexes_rows_and_drops_footer(tmp_path):
    path = Path(tmp_path, "2026-10-05_ATS_T1_2026-10-14 10:00:00.csv")
    path.write_text(WEEKLY)
    df = upsert.read_frame(path)
    assert df.index.names == [
        sql.WEEK,
        sql.CATEGORY,
        sql.REPORT_TYPE,
        sql.SYMBOL,
        sql.VENUE_DESCRIPTION,
    ]
    assert list(df.index.get_level_values(sql.SYMBOL)) == ["AAPL", "MSFT"]
    assert df["Shares"].sum() == 3000
    assert df.index[0][:2] == (pd.Timestamp("2026-10-05"), "ATS")


def test_otc_table_creates_with_composite_key():
    engine = sa.create_engine("sqlite://")
    md = sa.MetaData()
    table = sql.finra_otc_table(md)
    md.create_all(engine)
    assert [c.name for c in table.primary_key] == [
        sql.WEEK,
        sql.CATEGORY,
        sql.REPORT_TYPE,
        sql.SYMBOL,
        sql.VENUE_DESCRIPTION,
    ]
    assert sa.inspect(engine).has_table(sql.OTC_DATA)
