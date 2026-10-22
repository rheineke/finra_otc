import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import retrieve
import to_parquet

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

ATS_FILE = """\
Report_Type|Symbol|Issue_Description|ATS_Description|ATS_MPID|Shares|\
Shares_Last_Updated|Trades|Trades_Last_Updated
NMS Tier 1|AAPL|Apple Inc.|SOME ATS|ABCD|1000|28-JUL-14|10|28-JUL-14
NMS Tier 1|MSFT|Microsoft Corp.|SOME ATS|ABCD|2000|2014-07-29|20|2014-07-29
2"""

OTC_FILE = """\
Report_Type|Symbol|Issue_Description|OTC_Non-ATS_Description|Shares|\
Shares_Last_Updated|Trades|Trades_Last_Updated
OTCE|AABB|Asia Broadband Inc|De Minimis Firms|468069|06-AUG-18|41|06-AUG-18
1"""


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
    assert list(df[to_parquet.CATEGORY]) == ["ATS", "ATS", "OTC", "OTC"]
    assert list(df[retrieve.DOWNLOAD]) == [
        "/files/ATS/1",
        "/files/ATS/2",
        "/files/OTC/1",
        "/files/OTC/2",
    ]
    assert df[to_parquet.WEEK].iloc[0] == pd.Timestamp("2026-10-05")
    assert df[retrieve.LAST_UPDATED].iloc[1] == pd.Timestamp("2026-10-07 09:30")


def test_parser_reads_date():
    args = retrieve.get_parser().parse_args(["2026-10-01"])
    assert args.earliest_updated_date == dt.datetime(2026, 10, 1)


def test_year_string():
    assert to_parquet.year_string("2026")
    assert not to_parquet.year_string("tmp")


def write(folder, name, text):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(text)
    return path


def test_iterdata_skips_non_year_folders(tmp_path):
    for folder, name in [("2025", "a.csv"), ("2026", "b.csv"), ("misc", "c.csv")]:
        write(tmp_path / folder, name, "")
    names = sorted(p.name for p in to_parquet.iterdata(tmp_path))
    assert names == ["a.csv", "b.csv"]


def test_parse_filename():
    path = Path("2014-06-23_ATS_NMS Tier 1_2015-10-07 00:00:00.csv")
    assert to_parquet.parse_filename(path) == (
        dt.date(2014, 6, 23),
        "ATS",
        "NMS Tier 1",
        dt.datetime(2015, 10, 7),
    )


def test_latest_files_keep_the_newest_version_of_each_week():
    names = [
        "2014-06-30_ATS_OTC_2015-10-07 00:00:00.csv",
        "2014-06-23_ATS_OTC_2015-12-04 00:00:00.csv",
        "2014-06-23_ATS_OTC_2015-10-07 00:00:00.csv",
        "2014-06-23_OTC_OTC_2015-10-07 00:00:00.csv",
    ]
    assert [p.name for p in to_parquet.latest_files(map(Path, names))] == [
        names[1],
        names[3],
        names[0],
    ]


def test_read_table_ats_file(tmp_path):
    path = write(
        tmp_path, "2014-07-21_ATS_NMS Tier 1_2015-09-28 00:00:00.csv", ATS_FILE
    )
    table = to_parquet.read_table(path)
    assert table.schema == to_parquet.SCHEMA
    rows = table.to_pylist()
    assert [r[to_parquet.SYMBOL] for r in rows] == ["AAPL", "MSFT"]  # no footer
    first = rows[0]
    assert first[to_parquet.WEEK] == dt.date(2014, 7, 21)
    assert first[to_parquet.CATEGORY] == "ATS"
    assert first[to_parquet.VENUE_DESCRIPTION] == "SOME ATS"
    assert first["Shares_Last_Updated"] == dt.date(2014, 7, 28)
    assert rows[1]["Trades_Last_Updated"] == dt.date(2014, 7, 29)
    assert sum(r["Shares"] for r in rows) == 3000


def test_read_table_otc_file_has_no_mpid(tmp_path):
    path = write(tmp_path, "2018-07-02_OTC_OTC_2018-08-10 00:00:00.csv", OTC_FILE)
    (row,) = to_parquet.read_table(path).to_pylist()
    assert row[to_parquet.VENUE_DESCRIPTION] == "De Minimis Firms"
    assert row["ATS_MPID"] is None
    assert row["Shares"] == 468069


def test_read_table_rejects_malformed_rows(tmp_path):
    bad = ATS_FILE.replace("\n2", "\nnot|a|row")
    path = write(tmp_path, "2014-07-21_ATS_NMS Tier 1_2015-09-28 00:00:00.csv", bad)
    with pytest.raises(pa.ArrowInvalid):
        to_parquet.read_table(path)


def test_main_writes_one_parquet_file(tmp_path):
    data = tmp_path / "data"
    write(data / "2014", "2014-07-21_ATS_NMS Tier 1_2015-09-28 00:00:00.csv", ATS_FILE)
    write(data / "2014", "2014-07-21_ATS_NMS Tier 1_2015-10-07 00:00:00.csv", ATS_FILE)
    write(data / "2018", "2018-07-02_OTC_OTC_2018-08-10 00:00:00.csv", OTC_FILE)
    out = tmp_path / "otc.parquet"
    to_parquet.main(["--data", str(data), "--out", str(out)])
    table = pq.read_table(out)
    assert table.schema == to_parquet.SCHEMA
    assert table.num_rows == 3  # the older 2014 version is skipped
    assert pq.ParquetFile(out).num_row_groups == 2
    assert not out.with_name("otc.parquet.tmp").exists()


class DownloadSession(FakeSession):
    def __init__(self):
        self.downloads = []

    def get(self, url):
        if "/files/" in url:
            self.downloads.append(url)
            return SimpleNamespace(text=f"contents of {url}")
        return super().get(url)


def test_main_downloads_updated_files_once(tmp_path, monkeypatch, capsys):
    session = DownloadSession()
    monkeypatch.setattr(retrieve, "initialize_session", lambda: session)
    monkeypatch.chdir(tmp_path)
    existing = Path("data/2026/2026-10-05_OTC_T1_2026-10-14 10:00:00.csv")
    existing.parent.mkdir(parents=True)
    existing.write_text("already here")

    retrieve.main(["2026-10-10"])  # only the rows updated on 10/14

    assert session.downloads == [retrieve.BASE_URL + "/files/ATS/1"]
    saved = Path("data/2026/2026-10-05_ATS_T1_2026-10-14 10:00:00.csv")
    assert saved.read_text() == "contents of " + retrieve.BASE_URL + "/files/ATS/1"
    assert existing.read_text() == "already here"
    assert "2/2" in capsys.readouterr().err  # tqdm counted both files
