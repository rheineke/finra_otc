"""FINRA Query API (api.finra.org), group otcMarket: capture every partition.

The otcMarket datasets are public and need no credentials. A synchronous query
returns at most 5,000 records per page, and offsets stop at 500,000, so each
dataset is captured one partition at a time (for example one week and tier of
the weekly summary) and paged with ``offset``. Pages come back as CSV with every
value quoted; an unquoted empty value is null.

Each page is saved exactly as received. A partition's pages are saved together,
and only after all of them have arrived and been checked: same header, the
expected number of rows, and the same ``record-total`` on every page.
"""

import csv
import io
import json
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .http import HttpClient, HttpError, HttpResponse
from .raw import Capture, RawStore, check_name

BASE_URL = "https://api.finra.org"
GROUP = "otcMarket"
CSV = "text/plain"
MAX_PAGE = 5000
MAX_OFFSET = 500_000
FINRA_TZ = ZoneInfo("America/New_York")
UNPARTITIONED_KEY = "all"


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    # Rows per page; wide datasets use less to stay under the 3 MB response cap.
    page_size: int = MAX_PAGE
    # Dataset listing when each partition was last republished, if any.
    details: str | None = None
    # Partitions dated within this many days are fetched again on every run
    # (kept only if they changed), for datasets without a details listing.
    refresh_days: int = 0


# Details datasets come first: the others use them to find republished data.
DATASETS = [
    Dataset("weeklyDownloadDetails", "When each weekly file was last updated"),
    Dataset("monthlyDownloadDetails", "When each monthly file was last updated"),
    Dataset("ATSBlocksDownloadDetails", "When each ATS blocks file was last updated"),
    Dataset("OTCBlocksDownloadDetails", "When each OTC blocks file was last updated"),
    Dataset(
        "weeklySummary",
        "ATS and non-ATS (OTC) weekly volume by firm and symbol; rolling year",
        details="weeklyDownloadDetails",
    ),
    Dataset(
        "weeklySummaryHistoric",
        "The weekly summary beyond the rolling year (a rolling four years)",
        details="weeklyDownloadDetails",
    ),
    Dataset(
        "monthlySummary",
        "Non-ATS (OTC) monthly volume by firm and symbol, since 2014",
        details="monthlyDownloadDetails",
    ),
    Dataset(
        "blocksSummary",
        "ATS block-trade statistics by ATS and size bucket, monthly",
        details="ATSBlocksDownloadDetails",
    ),
    Dataset(
        "otcBlocksSummary",
        "Non-ATS (OTC) block-trade statistics by firm and size bucket, monthly",
        details="OTCBlocksDownloadDetails",
    ),
    Dataset(
        "regShoDaily",
        "Reg SHO daily short-sale volume by symbol and facility; rolling year",
        refresh_days=14,
    ),
    Dataset(
        "thresholdList",
        "Reg SHO and Rule 4320 threshold securities, daily",
        refresh_days=14,
    ),
    Dataset(
        "consolidatedShortInterest",
        "Short interest, all exchange-listed and OTC securities, twice monthly",
        refresh_days=45,
    ),
    Dataset(
        "equityShortInterest",
        "OTC equity short interest, twice monthly, 2016-2022 (superseded)",
        refresh_days=45,
    ),
    Dataset(
        "otcDailyList",
        "OTC daily list: symbol changes, corporate actions and dividends",
        page_size=2000,
        refresh_days=14,
    ),
]
BY_NAME = {d.name: d for d in DATASETS}


class CaptureError(RuntimeError):
    pass


class PartitionChanged(CaptureError):
    """record-total changed between pages: FINRA republished mid-capture."""


def data_url(dataset: str) -> str:
    return f"{BASE_URL}/data/group/{GROUP}/name/{dataset}"


def metadata_url(dataset: str) -> str:
    return f"{BASE_URL}/metadata/group/{GROUP}/name/{dataset}"


def partitions_url(dataset: str) -> str:
    return f"{BASE_URL}/partitions/group/{GROUP}/name/{dataset}"


def _json(response: HttpResponse) -> Any:
    if not (response.header("Content-Type") or "").startswith("application/json"):
        raise CaptureError(
            f"{response.url} returned {response.header('Content-Type')}, not JSON"
        )
    return json.loads(response.content)


def partition_key(values: Iterable[str]) -> str:
    values = list(values)
    if not values:
        return UNPARTITIONED_KEY
    return check_name("_".join(values))


def query_body(
    fields: list[str], values: list[str], limit: int, offset: int
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "quoteValues": True,
        "delimiter": ",",
    }
    if fields:
        body["compareFilters"] = [
            {"compareType": "equal", "fieldName": f, "fieldValue": v}
            for f, v in zip(fields, values, strict=True)
        ]
    return body


def _record_total(response: HttpResponse) -> int:
    value = response.header("record-total")
    if value is None or not value.strip().isdigit():
        raise CaptureError(f"{response.url}: no record-total header ({value!r})")
    return int(value)


def parse_page(content: bytes) -> tuple[list[str], int]:
    """(header, number of data rows) of one CSV page."""
    if not content:
        return [], 0
    text = content.decode("utf-8")
    rows = csv.reader(io.StringIO(text, newline=""))
    header = next(rows, [])
    return header, sum(1 for _ in rows)


_File = tuple[str, bytes, Mapping[str, Any]]


def fetch_partition(
    client: HttpClient,
    dataset: Dataset,
    fields: list[str],
    values: list[str],
) -> tuple[list[tuple[str, bytes, Mapping[str, Any]]], dict[str, Any]]:
    """Every page of one partition, checked; (files, capture meta)."""
    url = data_url(dataset.name)
    files: list[tuple[str, bytes, Mapping[str, Any]]] = []
    header: list[str] | None = None
    total: int | None = None
    offset = 0
    while True:
        body = query_body(fields, values, dataset.page_size, offset)
        response = client.post_json(url, body, CSV)
        page_total = _record_total(response)
        if total is None:
            total = page_total
            if total > MAX_OFFSET + dataset.page_size:
                raise CaptureError(
                    f"{dataset.name} {values}: {total:,} records is more than one "
                    f"query can page through ({MAX_OFFSET:,} offset limit)"
                )
        elif page_total != total:
            raise PartitionChanged(
                f"{dataset.name} {values}: record-total went from {total:,} to "
                f"{page_total:,} at offset {offset:,}"
            )
        if response.status == 204 or not response.content:
            if total != 0 or offset != 0:
                raise CaptureError(
                    f"{dataset.name} {values}: empty page at offset {offset:,} "
                    f"of {total:,}"
                )
            break
        content_type = response.header("Content-Type") or ""
        if not content_type.startswith(CSV):
            raise CaptureError(f"{url} returned {content_type}, expected {CSV}")
        page_header, rows = parse_page(response.content)
        if header is None:
            header = page_header
        elif page_header != header:
            raise CaptureError(f"{dataset.name} {values}: header changed at {offset:,}")
        expected = min(dataset.page_size, total - offset)
        if rows != expected:
            raise CaptureError(
                f"{dataset.name} {values}: {rows:,} rows at offset {offset:,}, "
                f"expected {expected:,}"
            )
        files.append(
            (
                f"offset-{offset:07d}.csv",
                response.content,
                {"offset": offset, "rows": rows, "body": body},
            )
        )
        offset += rows
        if offset >= total:
            break
    meta = {
        "url": url,
        "partition": dict(zip(fields, values, strict=True)),
        "record_total": total,
        "page_size": dataset.page_size,
    }
    return files, meta


def finra_date(timestamp: str) -> date:
    """The date in New York (FINRA's dates) of an ISO 8601 UTC timestamp."""
    return datetime.fromisoformat(timestamp).astimezone(FINRA_TZ).date()


def last_updates(
    rows: Iterable[Mapping[str, str | None]], fields: list[str]
) -> Callable[[list[str]], date | None]:
    """Look up when FINRA last updated a partition, from a details dataset.

    Rows match a partition on the partition fields they share with it. Where
    none match on all of them (the weekly summary's NA and NMS tiers have no
    details rows), rows matching on the date field alone are used.
    """
    rows = list(rows)
    shared = [f for f in fields if rows and f in rows[0]]
    exact: dict[tuple[str | None, ...], date] = {}
    by_date: dict[str | None, date] = {}
    for row in rows:
        updated = row.get("lastUpdateDate")
        if not updated:
            continue
        d = date.fromisoformat(updated)
        k = tuple(row.get(f) for f in shared)
        exact[k] = max(exact.get(k, d), d)
        first = row.get(fields[0]) if fields else None
        by_date[first] = max(by_date.get(first, d), d)

    def lookup(values: list[str]) -> date | None:
        if not shared:
            return None
        partition = dict(zip(fields, values, strict=True))
        k = tuple(partition[f] for f in shared)
        if k in exact:
            return exact[k]
        return by_date.get(values[0])

    return lookup


def details_rows(store: RawStore, details: str) -> list[dict[str, str | None]]:
    """Rows of the newest capture of a details dataset."""
    capture = store.latest(f"api/{details}").get(UNPARTITIONED_KEY)
    if capture is None:
        return []
    return [row for f in capture.files for row in csv_rows(store.read(f))]


def csv_rows(content: bytes) -> list[dict[str, str | None]]:
    """Rows of a quoted CSV page, with unquoted empty values as None."""
    if not content:
        return []
    reader = csv.reader(
        io.StringIO(content.decode("utf-8"), newline=""),
        quoting=csv.QUOTE_NOTNULL,
    )
    header = next(reader)
    return [dict(zip(header, row, strict=True)) for row in reader]


@dataclass(frozen=True)
class Plan:
    values: list[str]
    reason: str


def plan(
    partitions: list[list[str]],
    latest: Mapping[str, Capture],
    dataset: Dataset,
    *,
    updated: Callable[[list[str]], date | None],
    today: date,
    since: date | None = None,
    until: date | None = None,
    refresh_since: date | None = None,
) -> list[Plan]:
    """Partitions to fetch: new, republished, recent, or asked for again."""
    todo = []
    refresh_from = (
        today - timedelta(days=dataset.refresh_days) if dataset.refresh_days else None
    )
    for values in sorted(partitions):
        first = date.fromisoformat(values[0]) if values else None
        if first is not None:
            if since is not None and first < since:
                continue
            if until is not None and first > until:
                continue
        capture = latest.get(partition_key(values))
        if capture is None:
            todo.append(Plan(values, "new"))
            continue
        captured = finra_date(capture.captured_at)
        when = updated(values)
        if first is None:
            todo.append(Plan(values, "unpartitioned"))
        elif refresh_since is not None and first >= refresh_since:
            todo.append(Plan(values, "refresh"))
        # FINRA dates updates by day, so a same-day update may follow a capture.
        elif when is not None and when >= captured:
            todo.append(Plan(values, f"updated {when}"))
        elif refresh_from is not None and first >= refresh_from:
            todo.append(Plan(values, "recent"))
    return todo


@dataclass
class Result:
    dataset: str
    planned: int = 0
    saved: int = 0
    unchanged: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def _fetch_with_retries(
    client: HttpClient, dataset: Dataset, fields: list[str], item: Plan, retries: int
) -> tuple[list[tuple[str, bytes, Mapping[str, Any]]], dict[str, Any]]:
    for attempt in range(retries + 1):
        try:
            return fetch_partition(client, dataset, fields, item.values)
        except PartitionChanged:
            if attempt == retries:
                raise
    raise AssertionError("unreachable")


def capture_catalog(
    client: HttpClient, store: RawStore, dataset: str, now: datetime
) -> tuple[dict[str, Any], list[list[str]]]:
    """Fetch (and save, when changed) a dataset's metadata and partition list."""
    source = f"api_catalog/{dataset}"
    latest = store.latest(source)
    out: dict[str, Any] = {}
    for key, url in (
        ("metadata", metadata_url(dataset)),
        ("partitions", partitions_url(dataset)),
    ):
        response = client.get(url)
        out[key] = _json(response)
        previous = latest.get(key)
        sha = Capture.sha_of(response.content)
        if previous is None or previous.sha256s != [sha]:
            meta = {"url": url, "content_type": response.header("Content-Type")}
            files: list[tuple[str, bytes, dict[str, Any]]] = [
                (f"{key}.json", response.content, {})
            ]
            store.save(source, key, files, meta=meta, now=now)
    available = out["partitions"].get("availablePartitions", [])
    partitions = [p["partitions"] for p in available]
    return out["metadata"], partitions


def capture_dataset(
    client: HttpClient,
    store: RawStore,
    dataset: Dataset,
    *,
    since: date | None = None,
    until: date | None = None,
    refresh_since: date | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    progress: Callable[[str], None] = lambda _: None,
    retries: int = 2,
    workers: int = 4,
) -> Result:
    source = f"api/{dataset.name}"
    metadata, partitions = capture_catalog(client, store, dataset.name, now())
    fields: list[str] = metadata.get("partitionFields") or []
    if not fields:
        partitions = [[]]
    elif any(len(p) != len(fields) for p in partitions):
        raise CaptureError(f"{dataset.name}: partitions don't match {fields}")
    if dataset.details is not None:
        updated = last_updates(details_rows(store, dataset.details), fields)
    else:
        updated = last_updates([], fields)
    latest = store.latest(source)
    todo = plan(
        partitions,
        latest,
        dataset,
        updated=updated,
        today=now().astimezone(FINRA_TZ).date(),
        since=since,
        until=until,
        refresh_since=refresh_since,
    )
    result = Result(dataset.name, planned=len(todo))

    def fetch(item: Plan) -> tuple[list[_File], dict[str, Any]]:
        return _fetch_with_retries(client, dataset, fields, item, retries)

    # Partitions are fetched in parallel and saved in order, by this thread only.
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        window: deque[tuple[Plan, Future[tuple[list[_File], dict[str, Any]]]]] = deque()
        pending = iter(todo)
        done = 0
        while True:
            while len(window) < 2 * max(1, workers):
                item = next(pending, None)
                if item is None:
                    break
                window.append((item, pool.submit(fetch, item)))
            if not window:
                break
            item, future = window.popleft()
            done += 1
            key = partition_key(item.values)
            label = f"{dataset.name} {done}/{len(todo)} {key}"
            try:
                files, meta = future.result()
            except (CaptureError, HttpError, ValueError) as e:
                result.failed += 1
                result.errors.append(f"{key}: {e}")
                progress(f"{label}: FAILED {e}")
                continue
            previous = latest.get(key)
            if previous is not None and previous.sha256s == [
                Capture.sha_of(content) for _, content, _ in files
            ]:
                result.unchanged += 1
                progress(f"{label}: unchanged")
                continue
            meta["reason"] = item.reason
            store.save(source, key, files, meta=meta, now=now())
            result.saved += 1
            progress(f"{label}: {meta['record_total']:,} rows")
    return result
