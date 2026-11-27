import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import requests
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


def test_network_errors_fail_one_partition_not_the_run(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(2))
    parts = {
        "availablePartitions": [
            {"partitions": ["2026-09-14", "T1"]},
            {"partitions": ["2026-09-21", "T1"]},
        ]
    }
    client.routes[api.partitions_url("weeklySummary")] = response(
        "p", json.dumps(parts).encode(), headers={"Content-Type": "application/json"}
    )
    inner = pager(rows(2))

    def flaky(body: Mapping[str, Any] | None) -> HttpResponse:
        assert body is not None
        if body["compareFilters"][0]["fieldValue"] == "2026-09-14":
            raise requests.exceptions.ChunkedEncodingError("cut short")
        return inner(body)

    client.posts[api.data_url("weeklySummary")] = flaky
    r = api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    assert (r.saved, r.failed) == (1, 1)
    assert set(store.latest("api/weeklySummary")) == {"2026-09-21_T1"}


def test_unchanged_refetch_stops_republished_refetching(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(2))
    details = (
        b'"lastUpdateDate","tierIdentifier","summaryTypeCode","weekStartDate"\n'
        b'"2026-10-24","T1","ATS_W_SMBL","2026-09-14"\n'
    )
    store.save(
        "api/weeklyDownloadDetails",
        "all",
        [("offset-0000000.csv", details, {"rows": 1})],
        now=NOW,
    )
    weekly = api.Dataset(
        "weeklySummary", "t", page_size=3, details="weeklyDownloadDetails"
    )
    # Captured on the day of FINRA's update: fetched again the next day...
    api.capture_dataset(client, store, weekly, now=lambda: NOW)
    day2 = datetime(2026, 10, 25, 16, tzinfo=UTC)
    r = api.capture_dataset(client, store, weekly, now=lambda: day2)
    assert (r.planned, r.unchanged) == (1, 1)
    # ...but not every day after that.
    day3 = datetime(2026, 10, 26, 16, tzinfo=UTC)
    assert api.capture_dataset(client, store, weekly, now=lambda: day3).planned == 0


def test_an_empty_answer_doesnt_replace_rows(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(2))
    api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    client.posts[api.data_url("weeklySummary")] = pager([])
    r = api.capture_dataset(
        client, store, WEEKLY, refresh_since=date(2026, 1, 1), now=lambda: NOW
    )
    assert r.failed == 1 and "no rows" in r.errors[0]
    assert store.latest("api/weeklySummary")["2026-09-14_T1"].meta["record_total"] == 2


def test_a_damaged_capture_is_replaced_by_an_identical_fetch(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(2))
    api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    first = store.latest("api/weeklySummary")["2026-09-14_T1"]
    store.path(first.files[0]).write_bytes(b"rot")
    later = datetime(2026, 10, 25, tzinfo=UTC)
    r = api.capture_dataset(
        client, store, WEEKLY, refresh_since=date(2026, 1, 1), now=lambda: later
    )
    assert r.saved == 1
    fresh = store.latest("api/weeklySummary")["2026-09-14_T1"]
    assert store.read(fresh.files[0]) == csv_page(rows(2))


def test_catalog_ignores_request_ids(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(1))
    url = api.partitions_url("weeklySummary")
    js = {"Content-Type": "application/json"}
    # An unpartitioned dataset, like the details datasets.
    meta_url = api.metadata_url("weeklySummary")
    meta = {"fields": [{"name": "MPID", "type": "String"}]}
    client.routes[meta_url] = response(meta_url, json.dumps(meta).encode(), headers=js)
    for request_id in ("a1", "b2"):
        body = {"message": "No partitions found", "requestId": request_id}
        client.routes[url] = response(url, json.dumps(body).encode(), headers=js)
        api.capture_catalog(client, store, "weeklySummary", NOW)
    partitions = [
        c for c in store.captures("api_catalog/weeklySummary") if c.key == "partitions"
    ]
    assert len(partitions) == 1


def test_metadata_without_fields_is_refused(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(1))
    url = api.metadata_url("weeklySummary")
    body = {"statusCode": 200, "message": "try later", "requestId": "x"}
    js = {"Content-Type": "application/json"}
    client.routes[url] = response(url, json.dumps(body).encode(), headers=js)
    with pytest.raises(api.CaptureError, match="no field list"):
        api.capture_catalog(client, store, "weeklySummary", NOW)
    assert store.latest("api_catalog/weeklySummary") == {}


def test_partition_values_follow_the_answers_own_field_order(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(1))
    url = api.partitions_url("weeklySummary")
    answer = {
        "partitionFields": ["tierIdentifier", "weekStartDate"],
        "availablePartitions": [{"partitions": ["T1", "2026-09-14"]}],
    }
    js = {"Content-Type": "application/json"}
    client.routes[url] = response(url, json.dumps(answer).encode(), headers=js)
    _, partitions = api.capture_catalog(client, store, "weeklySummary", NOW)
    assert partitions == [["2026-09-14", "T1"]]


def test_a_stopped_capture_abandons_fetches_in_flight() -> None:
    stop = threading.Event()
    stop.set()
    client = FakeClient(posts={api.data_url("weeklySummary"): pager(rows(7))})
    with pytest.raises(api.Stopped):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES, stop)
    assert client.calls == []


def test_values_over_csvs_default_field_limit() -> None:
    big = "x" * 200_000
    page = f'"a"\n"{big}"\n'.encode()
    assert api.parse_page(page) == (["a"], 1)
    assert api.csv_rows(page) == [{"a": big}]


@pytest.mark.parametrize(
    "answer",
    [
        {"message": "No partitions found for specified dataset.", "requestId": "x"},
        {"availablePartitions": None},
        {"availablePartitions": [{"nope": 1}]},
        [],
    ],
)
def test_bad_partitions_answers_fail_and_save_nothing(
    tmp_path: Path, answer: Any
) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(1))
    url = api.partitions_url("weeklySummary")
    js = {"Content-Type": "application/json"}
    client.routes[url] = response(url, json.dumps(answer).encode(), headers=js)
    with pytest.raises(api.CaptureError):
        api.capture_catalog(client, store, "weeklySummary", NOW)
    assert store.latest("api_catalog/weeklySummary") == {}


def test_accept_empty(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(2))
    api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    client.posts[api.data_url("weeklySummary")] = pager([])
    r = api.capture_dataset(
        client,
        store,
        WEEKLY,
        refresh_since=date(2026, 1, 1),
        accept_empty=True,
        now=lambda: NOW.replace(hour=17),
    )
    assert r.saved == 1


def test_fetch_partition_detects_a_changed_header() -> None:
    inner = pager(rows(5))

    def handle(body: Mapping[str, Any] | None) -> HttpResponse:
        assert body is not None
        r = inner(body)
        if body["offset"]:
            renamed = r.content.replace(b'"MPID"', b'"mpid"', 1)
            return HttpResponse(r.url, r.status, r.headers, renamed)
        return r

    client = FakeClient(posts={api.data_url("weeklySummary"): handle})
    with pytest.raises(api.CaptureError, match="header changed at 3"):
        api.fetch_partition(client, WEEKLY, FIELDS, VALUES)


def test_a_partition_republished_mid_capture_is_fetched_again(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    client = catalog_client(rows(7))
    before, after = pager(rows(7)), pager(rows(8))
    offsets: list[int] = []

    def republished(body: Mapping[str, Any] | None) -> HttpResponse:
        assert body is not None
        offsets.append(body["offset"])
        # FINRA adds a row after the first page: record-total goes 7 -> 8.
        return before(body) if len(offsets) == 1 else after(body)

    client.posts[api.data_url("weeklySummary")] = republished
    r = api.capture_dataset(client, store, WEEKLY, now=lambda: NOW)
    assert (r.saved, r.failed) == (1, 0)
    assert offsets == [0, 3, 0, 3, 6]
    capture = store.latest("api/weeklySummary")["2026-09-14_T1"]
    assert capture.meta["record_total"] == 8


@dataclass
class CancellableClient(FakeClient):
    """Has the event RequestsClient's retry waits watch."""

    cancelled: threading.Event = field(default_factory=threading.Event)


def test_an_error_stops_the_fetches_in_flight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = RawStore(tmp_path)
    parts = {
        "availablePartitions": [
            {"partitions": ["2026-09-14", "T1"]},
            {"partitions": ["2026-09-21", "T1"]},
        ]
    }
    routes = catalog_client([]).routes
    routes[api.partitions_url("weeklySummary")] = response(
        "p", json.dumps(parts).encode(), headers={"Content-Type": "application/json"}
    )
    client = CancellableClient(routes=routes)
    one, seven = pager(rows(1)), pager(rows(7, week="2026-09-21"))
    slow_started = threading.Event()
    slow_offsets: list[int] = []

    def handle(body: Mapping[str, Any] | None) -> HttpResponse:
        assert body is not None
        if body["compareFilters"][0]["fieldValue"] == "2026-09-21":
            slow_offsets.append(body["offset"])
            slow_started.set()
            client.cancelled.wait(10)  # answers once the capture is stopping
            return seven(body)
        slow_started.wait(10)
        return one(body)

    client.posts[api.data_url("weeklySummary")] = handle
    real_save = store.save

    def save(source: str, key: str, files: Any, **kwargs: Any) -> Any:
        if source == "api/weeklySummary":
            raise OSError("disk full")
        return real_save(source, key, files, **kwargs)

    monkeypatch.setattr(store, "save", save)
    with pytest.raises(OSError, match="disk full"):
        api.capture_dataset(client, store, WEEKLY, now=lambda: NOW, workers=2)
    assert slow_offsets == [0]  # stopped before its next page
