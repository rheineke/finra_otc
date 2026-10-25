import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from fakes import FakeClient, response

from finra_otc import api
from finra_otc.http import HttpResponse
from finra_otc.raw import RawStore

NOW = datetime(2026, 10, 24, 16, 0, tzinfo=UTC)
HEADER = '"weekStartDate","tierIdentifier","MPID","lastUpdateDate"\n'


def csv_page(rows: list[list[str | None]]) -> bytes:
    lines = [",".join("" if v is None else f'"{v}"' for v in row) for row in rows]
    return (HEADER + "".join(line + "\n" for line in lines)).encode()


def rows(n: int, week: str = "2026-09-14", tier: str = "T1") -> list[list[str | None]]:
    return [[week, tier, f"M{i:03d}", "2026-10-05"] for i in range(n)]


def pager(all_rows: list[list[str | None]], total: int | None = None) -> Any:
    """A POST handler paging ``all_rows`` like the FINRA API."""

    def handle(body: Mapping[str, Any] | None) -> HttpResponse:
        assert body is not None
        offset, limit = body["offset"], body["limit"]
        page = all_rows[offset : offset + limit]
        headers = {
            "Content-Type": "text/plain",
            "record-total": str(len(all_rows) if total is None else total),
        }
        if not all_rows:
            return response("u", b"", status=204, headers=headers)
        return response("u", csv_page(page), headers=headers)

    return handle


WEEKLY = api.Dataset("weeklySummary", "test", page_size=3)
FIELDS = ["weekStartDate", "tierIdentifier"]
VALUES = ["2026-09-14", "T1"]


def test_query_body_filters_on_every_partition_field() -> None:
    body = api.query_body(FIELDS, VALUES, 5000, 10)
    assert body["compareFilters"] == [
        {
            "compareType": "equal",
            "fieldName": "weekStartDate",
            "fieldValue": "2026-09-14",
        },
        {"compareType": "equal", "fieldName": "tierIdentifier", "fieldValue": "T1"},
    ]
    assert body["offset"] == 10 and body["quoteValues"] is True
    assert "compareFilters" not in api.query_body([], [], 5000, 0)


def test_parse_page_counts_rows_with_embedded_newlines() -> None:
    page = b'"a","b"\n"1","two\nlines"\n"3",\n'
    assert api.parse_page(page) == (["a", "b"], 2)


def test_fetch_partition_pages_through_all_rows() -> None:
    client = FakeClient(posts={api.data_url("weeklySummary"): pager(rows(7))})
    files, meta = api.fetch_partition(client, WEEKLY, FIELDS, VALUES)
    assert [f[0] for f in files] == [
        "offset-0000000.csv",
        "offset-0000003.csv",
        "offset-0000006.csv",
    ]
    assert [f[2]["rows"] for f in files] == [3, 3, 1]
    assert meta["record_total"] == 7
    assert meta["partition"] == {"weekStartDate": "2026-09-14", "tierIdentifier": "T1"}


def test_fetch_partition_empty() -> None:
    client = FakeClient(posts={api.data_url("weeklySummary"): pager([])})
    files, meta = api.fetch_partition(client, WEEKLY, FIELDS, VALUES)
    assert files == [] and meta["record_total"] == 0


def test_fetch_partition_rejects_short_page() -> None:
    client = FakeClient(posts={api.data_url("weeklySummary"): pager(rows(5), 7)})
    with pytest.raises(api.CaptureError, match="rows at offset"):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES)


def test_fetch_partition_detects_republication() -> None:
    totals = iter(["7", "8"])
    inner = pager(rows(7))

    def handle(body: Mapping[str, Any] | None) -> HttpResponse:
        r = inner(body)
        return HttpResponse(
            r.url, r.status, {**r.headers, "record-total": next(totals)}, r.content
        )

    client = FakeClient(posts={api.data_url("weeklySummary"): handle})
    with pytest.raises(api.PartitionChanged):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES)


def test_fetch_partition_refuses_partitions_past_the_offset_limit() -> None:
    client = FakeClient(
        posts={api.data_url("weeklySummary"): pager(rows(1), total=600_000)}
    )
    with pytest.raises(api.CaptureError, match="offset limit"):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES)


def test_fetch_partition_rejects_non_csv() -> None:
    def handle(body: Mapping[str, Any] | None) -> HttpResponse:
        return response(
            "u", b"<html>", headers={"Content-Type": "text/html", "record-total": "1"}
        )

    client = FakeClient(posts={api.data_url("weeklySummary"): handle})
    with pytest.raises(api.CaptureError, match="expected text/plain"):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES)


DETAILS = [
    {
        "weekStartDate": "2026-09-14",
        "tierIdentifier": "T1",
        "lastUpdateDate": "2026-10-05",
    },
    {
        "weekStartDate": "2026-09-14",
        "tierIdentifier": "T1",
        "lastUpdateDate": "2026-10-12",
    },
    {
        "weekStartDate": "2026-09-14",
        "tierIdentifier": "T2",
        "lastUpdateDate": "2026-10-06",
    },
]


def test_last_updates_matches_partition_fields() -> None:
    lookup = api.last_updates(DETAILS, FIELDS)
    assert lookup(["2026-09-14", "T1"]) == date(2026, 10, 12)
    assert lookup(["2026-09-14", "T2"]) == date(2026, 10, 6)
    # No details rows for the NA tier: use the newest update of that week.
    assert lookup(["2026-09-14", "NA"]) == date(2026, 10, 12)
    assert lookup(["2026-09-21", "T1"]) is None
    assert api.last_updates([], FIELDS)(["2026-09-14", "T1"]) is None


def _store_with(tmp_path: Path, key: str, captured: datetime) -> RawStore:
    store = RawStore(tmp_path)
    store.save("api/weeklySummary", key, [("f.csv", b"x", {})], now=captured)
    return store


def test_plan(tmp_path: Path) -> None:
    store = _store_with(
        tmp_path, "2026-09-14_T1", datetime(2026, 10, 10, 15, tzinfo=UTC)
    )
    latest = store.latest("api/weeklySummary")
    partitions = [["2026-09-14", "T1"], ["2026-09-21", "T1"], ["2026-01-05", "T1"]]

    def plan(**kw: Any) -> list[tuple[list[str], str]]:
        kw.setdefault("updated", lambda v: None)
        out = api.plan(partitions, latest, WEEKLY, today=date(2026, 10, 24), **kw)
        return [(p.values, p.reason) for p in out]

    assert plan() == [(["2026-01-05", "T1"], "new"), (["2026-09-21", "T1"], "new")]
    assert plan(since=date(2026, 9, 1), until=date(2026, 9, 20)) == []
    updated = plan(updated=lambda v: date(2026, 10, 12))
    assert (["2026-09-14", "T1"], "updated 2026-10-12") in updated
    # An update dated the capture's own day may have come after it.
    same_day = plan(updated=lambda v: date(2026, 10, 10))
    assert (["2026-09-14", "T1"], "updated 2026-10-10") in same_day
    assert (["2026-09-14", "T1"], "refresh") in plan(refresh_since=date(2026, 9, 1))


def test_plan_recent_window(tmp_path: Path) -> None:
    store = _store_with(tmp_path, "2026-10-20", datetime(2026, 10, 21, tzinfo=UTC))
    daily = api.Dataset("regShoDaily", "t", refresh_days=14)
    todo = api.plan(
        [["2026-10-20"], ["2026-01-02"]],
        {**store.latest("api/weeklySummary")},
        daily,
        updated=lambda v: None,
        today=date(2026, 10, 24),
    )
    assert [(p.values, p.reason) for p in todo] == [
        (["2026-01-02"], "new"),
        (["2026-10-20"], "recent"),
    ]


def test_finra_date_is_new_york() -> None:
    # 01:00 UTC on the 25th is still the 24th in New York.
    assert api.finra_date("2026-10-25T01:00:00+00:00") == date(2026, 10, 24)


def catalog_client(all_rows: list[list[str | None]]) -> FakeClient:
    meta = {
        "partitionFields": FIELDS,
        "fields": [
            {"name": n, "type": t}
            for n, t in [
                ("weekStartDate", "Date"),
                ("tierIdentifier", "String"),
                ("MPID", "String"),
                ("lastUpdateDate", "Date"),
            ]
        ],
    }
    parts = {"availablePartitions": [{"partitions": VALUES}]}
    js = {"Content-Type": "application/json"}
    return FakeClient(
        routes={
            api.metadata_url("weeklySummary"): response(
                "m", json.dumps(meta).encode(), headers=js
            ),
            api.partitions_url("weeklySummary"): response(
                "p", json.dumps(parts).encode(), headers=js
            ),
        },
        posts={api.data_url("weeklySummary"): pager(all_rows)},
    )


def test_capture_dataset_saves_then_skips_unchanged(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(4))
    r = api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    assert (r.planned, r.saved) == (1, 1)
    capture = store.latest("api/weeklySummary")["2026-09-14_T1"]
    assert capture.meta["record_total"] == 4 and len(capture.files) == 2
    assert set(store.latest("api_catalog/weeklySummary")) == {"metadata", "partitions"}

    # Captured, not updated since and not recent: nothing to do.
    later = datetime(2026, 12, 1, tzinfo=UTC)
    r = api.capture_dataset(client, store, WEEKLY, now=lambda: later)
    assert r.planned == 0
    # Asked for again but unchanged: nothing saved.
    r = api.capture_dataset(
        client, store, WEEKLY, refresh_since=date(2026, 1, 1), now=lambda: later
    )
    assert (r.planned, r.saved, r.unchanged) == (1, 0, 1)
    assert len(list(store.captures("api_catalog/weeklySummary"))) == 2


def test_capture_dataset_counts_failures_and_continues(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(4))
    client.posts[api.data_url("weeklySummary")] = pager(rows(2), total=4)
    r = api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    assert (r.saved, r.failed) == (0, 1)
    assert store.latest("api/weeklySummary") == {}
