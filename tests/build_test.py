import datetime as dt
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from finra_otc import build, cdn, legacy
from finra_otc.raw import RawStore

NOW = datetime(2026, 10, 24, 16, tzinfo=UTC)

METADATA: dict[str, Any] = {
    "partitionFields": ["weekStartDate"],
    "fields": [
        {"name": "weekStartDate", "type": "Date"},
        {"name": "MPID", "type": "String"},
        {"name": "firmCRDNumber", "type": "Number"},
        {"name": "totalWeeklyShareQuantity", "type": "Number"},
    ],
}


def save_catalog(store: RawStore, dataset: str, metadata: dict[str, Any]) -> None:
    content = json.dumps(metadata).encode()
    store.save(
        f"api_catalog/{dataset}", "metadata", [("metadata.json", content, {})], now=NOW
    )


def save_partition(
    store: RawStore, dataset: str, key: str, pages: list[bytes], now: datetime = NOW
) -> None:
    files = []
    total = 0
    for i, page in enumerate(pages):
        rows = page.count(b"\n") - 1
        files.append((f"offset-{i:07d}.csv", page, {"rows": rows}))
        total += rows
    store.save(f"api/{dataset}", key, files, meta={"record_total": total}, now=now)


PAGE = (
    b'"weekStartDate","MPID","firmCRDNumber","totalWeeklyShareQuantity"\n'
    b'"2026-09-14","ABCD",,"10.500000"\n'
    b'"2026-09-14","","79","2.000000"\n'
)


def test_build_api_types_and_nulls(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_catalog(store, "weeklySummary", METADATA)
    save_partition(store, "weeklySummary", "2026-09-14", [PAGE, PAGE])
    [r] = build.build_api(store, ["weeklySummary"])
    assert (r.built, r.rows, r.failed) == (1, 4, 0)
    table = pq.read_table(tmp_path / "parquet/api/weeklySummary/2026-09-14.parquet")
    assert table.schema.field("weekStartDate").type == pa.date32()
    assert table.schema.field("firmCRDNumber").type == pa.int64()
    assert table.schema.field("totalWeeklyShareQuantity").type == pa.float64()
    assert table.column("firmCRDNumber").to_pylist() == [None, 79, None, 79]
    # Unquoted and quoted empty values are both null.
    assert table.column("MPID").to_pylist() == ["ABCD", None, "ABCD", None]
    assert table.column("weekStartDate")[0].as_py() == dt.date(2026, 9, 14)

    # Unchanged inputs: nothing rebuilt.
    [r] = build.build_api(store, ["weeklySummary"])
    assert (r.built, r.current) == (0, 1)
    # A newer capture: rebuilt.
    save_partition(
        store, "weeklySummary", "2026-09-14", [PAGE], now=NOW.replace(hour=17)
    )
    [r] = build.build_api(store, ["weeklySummary"])
    assert (r.built, r.rows) == (1, 2)


@pytest.mark.parametrize(
    "row, error",
    [
        (b'"2026-09-14","A","7.5","1"\n', "firmCRDNumber"),
        (b'"2026-09-14 10:00:00.0","A","7","1"\n', "time of day"),
        (b'"14-SEP-26","A","7","1"\n', "weekStartDate"),
        (b'"2026-09-14","A","7","lots"\n', "totalWeeklyShareQuantity"),
    ],
)
def test_build_api_fails_on_values_it_cannot_convert(
    tmp_path: Path, row: bytes, error: str
) -> None:
    store = RawStore(tmp_path)
    save_catalog(store, "weeklySummary", METADATA)
    save_partition(
        store, "weeklySummary", "2026-09-14", [PAGE.split(b"\n")[0] + b"\n" + row]
    )
    [r] = build.build_api(store, ["weeklySummary"])
    assert r.failed == 1 and error in r.errors[0]
    assert not (tmp_path / "parquet/api/weeklySummary/2026-09-14.parquet").exists()


def test_build_api_fails_on_unknown_columns(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_catalog(store, "weeklySummary", METADATA)
    save_partition(store, "weeklySummary", "k", [b'"surprise"\n"1"\n'])
    [r] = build.build_api(store, ["weeklySummary"])
    assert r.failed == 1 and "surprise" in r.errors[0]


def test_build_api_empty_partition(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_catalog(store, "weeklySummary", METADATA)
    save_partition(store, "weeklySummary", "2026-09-14", [])
    [r] = build.build_api(store, ["weeklySummary"])
    assert r.built == 1
    table = pq.read_table(tmp_path / "parquet/api/weeklySummary/2026-09-14.parquet")
    assert table.num_rows == 0 and table.schema.names == [
        f["name"] for f in METADATA["fields"]
    ]


def test_otc_daily_list_dates_keep_their_times() -> None:
    assert build.api_type("otcDailyList", "exDate", "Date") == pa.timestamp("ms")
    assert build.api_type("otcDailyList", "calendarDay", "Date") == pa.date32()
    assert build.api_type("weeklySummary", "weekStartDate", "Date") == pa.date32()


def test_integer_fields() -> None:
    assert build.api_type("x", "totalWeeklyTradeCount", "Number") == pa.int64()
    assert build.api_type("x", "ATSBlockShareRank", "Number") == pa.int64()
    assert build.api_type("x", "OTCDailyListID", "Number") == pa.int64()
    assert build.api_type("x", "totalWeeklyShareQuantity", "Number") == pa.float64()


DAILY_2009 = b"Date|Symbol|ShortVolume|TotalVolume|Market\n20090803|NA|5|9|Q\n1\n"
DAILY = (
    b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
    b"20261022|A|1.5|0|3|B,Q,N\n20261023|B|2|1|4|Q\n2\n"
)


def save_daily(store: RawStore, facility: str, day: str, content: bytes) -> None:
    key = f"{facility}shvol{day.replace('-', '')}"
    store.save(
        cdn.DAILY_SOURCE,
        key,
        [(f"{key}.txt", content, {})],
        meta={"facility": facility, "date": day},
        now=NOW,
    )


def test_build_daily(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_daily(store, "FNSQ", "2009-08-03", DAILY_2009)
    save_daily(store, "CNMS", "2026-10-22", DAILY)
    r = build.build_daily(store)
    assert (r.built, r.rows, r.failed) == (2, 3, 0)
    old = pq.read_table(tmp_path / "parquet/cdn/regsho_daily/FNSQ/2009-08.parquet")
    # "NA" is a symbol, not a missing value.
    assert old.to_pylist() == [
        {
            "Date": dt.date(2009, 8, 3),
            "Symbol": "NA",
            "ShortVolume": 5.0,
            "ShortExemptVolume": None,
            "TotalVolume": 9.0,
            "Market": "Q",
            "Facility": "FNSQ",
        }
    ]
    assert build.build_daily(store).current == 2


def test_build_daily_checks_the_row_count_footer(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_daily(store, "CNMS", "2026-10-22", DAILY.replace(b"\n2\n", b"\n3\n"))
    r = build.build_daily(store)
    assert r.failed == 1 and "footer" in r.errors[0]


def test_build_daily_empty_file(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_daily(
        store,
        "FNRA",
        "2026-10-22",
        b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n",
    )
    assert build.build_daily(store).built == 1


MONTHLY_NEW = (
    b"MarketCenter|Symbol|Date|Time|ShortType|Size|Price|LinkIndicator|\n"
    b"B|GLD|20260901|04:01:02|S|5.000000|406.110000||\n"
    b"B|NA|20260901|16:00:00|E|1.5|1||\n"
    b"2\n"
)
MONTHLY_OLD = (
    b"MarketCenter|Symbol|Date|Time|ShortType|Size|Price|LinkIndicator\n"
    b"N|A|20090803|14:56:31|S|100|23.985| \n"
    b"1\n"
)


def save_monthly(store: RawStore, key: str, text: bytes) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"{key}.txt", text)
    scratch = store.scratch_dir(cdn.MONTHLY_SOURCE) / "x"
    scratch.write_bytes(buf.getvalue())
    store.save_download(cdn.MONTHLY_SOURCE, key, f"{key}.zip", scratch, "sha", now=NOW)


def test_build_monthly(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_monthly(store, "FNQCsh202609", MONTHLY_NEW)
    save_monthly(store, "FNYXsh200908", MONTHLY_OLD)
    r = build.build_monthly(store)
    assert (r.built, r.rows, r.failed) == (2, 3, 0), r.errors
    new = pq.read_table(tmp_path / "parquet/cdn/regsho_monthly/FNQCsh202609.parquet")
    assert new.schema.names == build.MONTHLY_SCHEMA.names
    first = new.to_pylist()[0]
    assert first["Time"] == dt.time(4, 1, 2) and first["Size"] == 5.0
    assert first["LinkIndicator"] is None
    assert new.column("Symbol").to_pylist() == ["GLD", "NA"]


def test_build_monthly_checks_the_footer(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    save_monthly(store, "FNQCsh202609", MONTHLY_NEW.replace(b"\n2\n", b"\n5\n"))
    r = build.build_monthly(store)
    assert r.failed == 1
    assert not list((tmp_path / "parquet/cdn/regsho_monthly").glob("*"))


ATS_FILE = (
    "Report_Type|Symbol|Issue_Description|ATS_Description|ATS_MPID|Shares|"
    "Shares_Last_Updated|Trades|Trades_Last_Updated\n"
    "NMS Tier 1|AAPL|Apple Inc.|SOME ATS|ABCD|1000|28-JUL-14|10|28-JUL-14\n"
    "NMS Tier 1|MSFT|Microsoft Corp.|SOME ATS|ABCD|2000|2014-07-29|20|2014-07-29\n"
    "2"
)
OTC_FILE = (
    "Report_Type|Symbol|Issue_Description|OTC_Non-ATS_Description|Shares|"
    "Shares_Last_Updated|Trades|Trades_Last_Updated\n"
    "OTCE|AABB|Asia Broadband Inc|De Minimis Firms|468069|06-AUG-18|41|06-AUG-18\n"
    "1"
)


def write_legacy(folder: Path, name: str, text: str) -> None:
    (folder / name[:4]).mkdir(parents=True, exist_ok=True)
    (folder / name[:4] / name).write_text(text)


def test_legacy_import_and_build(tmp_path: Path) -> None:
    src = tmp_path / "old"
    write_legacy(src, "2014-07-28_ATS_NMS Tier 1_2015-10-07 00:00:00.csv", ATS_FILE)
    # Republished: the newer file wins.
    write_legacy(
        src,
        "2014-07-28_ATS_NMS Tier 1_2015-12-04 00:00:00.csv",
        ATS_FILE.replace("1000", "1500"),
    )
    write_legacy(src, "2018-08-06_OTC_OTCE_2018-09-01 00:00:00.csv", OTC_FILE)
    store = RawStore(tmp_path / "data")
    assert legacy.import_legacy(store, src, now=lambda: NOW) == (3, 0)
    assert legacy.import_legacy(store, src, now=lambda: NOW) == (0, 3)
    r = build.build_legacy(store)
    assert (r.built, r.rows) == (2, 3)
    t2014 = pq.read_table(tmp_path / "data/parquet/legacy/weekly/2014.parquet")
    assert t2014.column("Shares").to_pylist() == [1500, 2000]
    assert t2014.column("Shares_Last_Updated")[0].as_py() == dt.date(2014, 7, 28)
    t2018 = pq.read_table(tmp_path / "data/parquet/legacy/weekly/2018.parquet")
    assert t2018.column("Venue_Description").to_pylist() == ["De Minimis Firms"]
    assert t2018.column("Category").to_pylist() == ["OTC"]


def test_legacy_parse_filename() -> None:
    info = legacy.parse_filename("2014-06-23_ATS_NMS Tier 1_2015-10-07 00:00:00")
    assert info.key == "2014-06-23_ATS_NMS-Tier-1_20151007T000000"
    with pytest.raises(ValueError):
        legacy.parse_filename("notes")
