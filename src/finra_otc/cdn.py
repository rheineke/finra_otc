"""Reg SHO short-sale files on FINRA's CDN (cdn.finra.org/equity/regsho).

Daily short-sale volume files go back to August 2009, while the API's
regShoDaily keeps only a rolling year. One file per trade date and facility:

    daily/{facility}shvol{YYYYMMDD}.txt      pipe-delimited, row-count footer

Monthly short-sale transaction files list every short sale, one ZIP per month
and facility. The Nasdaq Carteret TRF (FNSQ) splits a month into parts _1, _2...:

    monthly/{facility}sh{YYYYMM}[_{part}].zip

Missing files answer 403. The CDN has no index, so dates are enumerated
(weekdays for daily files) and misses are recorded in absent.jsonl so that
they aren't asked for again once FINRA has had time to post them.
"""

import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from .api import FINRA_TZ
from .http import HttpClient, HttpError
from .raw import Capture, RawStore

CDN_URL = "https://cdn.finra.org/equity/regsho"
DAILY_SOURCE = "cdn/regsho_daily"
MONTHLY_SOURCE = "cdn/regsho_monthly"
MISSING = frozenset({403, 404})

# Facility: first date to try. CNMS (consolidated NMS) and FNQC (Nasdaq Chicago
# TRF) begin in August 2018. FNSQ is the Nasdaq Carteret TRF, FNYX the NYSE TRF,
# FNRA the ADF and FORF the ORF (OTC securities).
DAILY_FACILITIES = {
    "CNMS": date(2018, 8, 1),
    "FNQC": date(2018, 8, 1),
    "FNSQ": date(2009, 8, 3),
    "FNYX": date(2009, 8, 3),
    "FNRA": date(2009, 8, 3),
    "FORF": date(2009, 8, 3),
}
MONTHLY_FACILITIES = {
    "FNQC": date(2018, 8, 1),
    "FNSQ": date(2009, 8, 1),
    "FNYX": date(2009, 8, 1),
    "FNRA": date(2009, 8, 1),
}
DAILY_HEADER = b"Date|Symbol|"
ZIP_MAGIC = b"PK\x03\x04"
MAX_PARTS = 20


@dataclass
class Result:
    source: str
    checked: int = 0
    saved: int = 0
    unchanged: int = 0
    absent: int = 0
    skipped: int = 0
    failed: int = 0
    bytes: int = 0
    errors: list[str] = field(default_factory=list)


def daily_name(facility: str, day: date) -> str:
    return f"{facility}shvol{day:%Y%m%d}"


def daily_url(facility: str, day: date) -> str:
    return f"{CDN_URL}/daily/{daily_name(facility, day)}.txt"


def monthly_name(facility: str, month: date, part: int | None) -> str:
    suffix = f"_{part}" if part is not None else ""
    return f"{facility}sh{month:%Y%m}{suffix}"


def monthly_url(facility: str, month: date, part: int | None) -> str:
    return f"{CDN_URL}/monthly/{monthly_name(facility, month, part)}.zip"


def weekdays(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def months(start: date, end: date) -> Iterator[date]:
    month = start.replace(day=1)
    while month <= end:
        yield month
        month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)


def _month_end(month: date) -> date:
    return (month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(1)


def _settled(checked_at: str, available_by: date) -> bool:
    """Was the file already overdue when it was found missing?"""
    return datetime.fromisoformat(checked_at).astimezone(FINRA_TZ).date() > (
        available_by
    )


def capture_daily(
    client: HttpClient,
    store: RawStore,
    *,
    since: date | None = None,
    until: date | None = None,
    facilities: list[str] | None = None,
    refresh_days: int = 10,
    grace_days: int = 7,
    recheck_absent: bool = False,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    progress: Callable[[str], None] = lambda _: None,
) -> Result:
    """Fetch daily files not yet captured; re-check recent ones for updates."""
    today = now().astimezone(FINRA_TZ).date()
    end = min(until or today, today)
    latest = store.latest(DAILY_SOURCE)
    absent = store.absences(DAILY_SOURCE)
    result = Result(DAILY_SOURCE)
    for facility in facilities or list(DAILY_FACILITIES):
        start = max(since or DAILY_FACILITIES[facility], DAILY_FACILITIES[facility])
        for day in weekdays(start, end):
            key = daily_name(facility, day)
            url = daily_url(facility, day)
            previous = latest.get(key)
            recent = day >= today - timedelta(days=refresh_days)
            if previous is not None and not recent:
                result.skipped += 1
                continue
            a = absent.get(key)
            if (
                previous is None
                and a is not None
                and not recheck_absent
                and _settled(a.checked_at, day + timedelta(days=grace_days))
            ):
                result.skipped += 1
                continue
            result.checked += 1
            try:
                if previous is not None:
                    etag = client.head(url).header("ETag")
                    if etag is not None and etag == previous.files[0].meta.get("etag"):
                        result.unchanged += 1
                        continue
                response = client.get(url)
            except HttpError as e:
                if e.status in MISSING:
                    if previous is None:
                        store.record_absence(DAILY_SOURCE, key, url, e.status, now())
                    result.absent += 1
                    continue
                result.failed += 1
                result.errors.append(f"{key}: {e}")
                continue
            if not response.content.startswith(DAILY_HEADER):
                result.failed += 1
                result.errors.append(f"{key}: not a short-sale volume file")
                continue
            if previous is not None and previous.sha256s == [
                Capture.sha_of(response.content)
            ]:
                result.unchanged += 1
                continue
            file_meta = {
                "url": response.url,
                "etag": response.header("ETag"),
                "last_modified": response.header("Last-Modified"),
                "content_type": response.header("Content-Type"),
            }
            store.save(
                DAILY_SOURCE,
                key,
                [(f"{key}.txt", response.content, file_meta)],
                meta={"facility": facility, "date": day.isoformat()},
                now=now(),
            )
            result.saved += 1
            result.bytes += len(response.content)
            progress(f"{key}: {len(response.content):,} bytes")
    return result


def capture_monthly(
    client: HttpClient,
    store: RawStore,
    *,
    since: date,
    until: date | None = None,
    facilities: list[str] | None = None,
    refresh_months: int = 2,
    grace_days: int = 45,
    recheck_absent: bool = False,
    dry_run: bool = False,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    progress: Callable[[str], None] = lambda _: None,
) -> Result:
    """Fetch monthly ZIPs (streamed to disk); ``dry_run`` only sizes them up."""
    today = now().astimezone(FINRA_TZ).date()
    last_month = today.replace(day=1) - timedelta(days=1)
    end = min(until or last_month, last_month)
    latest = store.latest(MONTHLY_SOURCE)
    absent = store.absences(MONTHLY_SOURCE)
    result = Result(MONTHLY_SOURCE)
    for facility in facilities or list(MONTHLY_FACILITIES):
        start = max(since, MONTHLY_FACILITIES[facility])
        for month in months(start, end):
            recent = month >= (
                today.replace(day=1) - timedelta(days=31 * refresh_months)
            )
            overdue = _month_end(month) + timedelta(days=grace_days)
            for part in [None, *range(1, MAX_PARTS + 1)]:
                key = monthly_name(facility, month, part)
                url = monthly_url(facility, month, part)
                previous = latest.get(key)
                a = absent.get(key)
                if previous is not None and not recent:
                    result.skipped += 1
                    if part is None:
                        break  # a whole-month file means no parts
                    continue
                if (
                    previous is None
                    and a is not None
                    and not recheck_absent
                    and _settled(a.checked_at, overdue)
                ):
                    result.skipped += 1
                    if part is None:
                        continue
                    break
                result.checked += 1
                try:
                    head = client.head(url)
                except HttpError as e:
                    if e.status not in MISSING:
                        result.failed += 1
                        result.errors.append(f"{key}: {e}")
                        break
                    if previous is None and not dry_run:
                        store.record_absence(MONTHLY_SOURCE, key, url, e.status, now())
                    result.absent += 1
                    if part is None:
                        continue  # the month may be split into parts
                    break
                etag = head.header("ETag")
                size = int(head.header("Content-Length") or 0)
                if previous is not None and etag == previous.files[0].meta.get("etag"):
                    result.unchanged += 1
                elif dry_run:
                    result.bytes += size
                    progress(f"{key}: {size:,} bytes")
                else:
                    try:
                        _download_monthly(client, store, facility, month, key, url, now)
                        result.saved += 1
                        result.bytes += size
                        progress(f"{key}: {size:,} bytes")
                    except (HttpError, OSError, ValueError) as e:
                        result.failed += 1
                        result.errors.append(f"{key}: {e}")
                if part is None:
                    break  # a whole-month file means no parts
    return result


def _download_monthly(
    client: HttpClient,
    store: RawStore,
    facility: str,
    month: date,
    key: str,
    url: str,
    now: Callable[[], datetime],
) -> Capture:
    scratch = store.scratch_dir(MONTHLY_SOURCE) / f"{key}.zip.part"
    try:
        download = client.download(url, scratch)
        with scratch.open("rb") as f:
            if f.read(4) != ZIP_MAGIC:
                raise ValueError(f"{url} is not a ZIP file")
        with zipfile.ZipFile(scratch) as z:
            bad = z.testzip()
            if bad is not None:
                raise ValueError(f"{url}: {bad} fails its CRC check")
        file_meta = {
            "url": download.url,
            "etag": download.header("ETag"),
            "last_modified": download.header("Last-Modified"),
            "content_type": download.header("Content-Type"),
        }
        return store.save_download(
            MONTHLY_SOURCE,
            key,
            f"{key}.zip",
            scratch,
            download.sha256,
            meta={"facility": facility, "month": f"{month:%Y-%m}"},
            file_meta=file_meta,
            now=now(),
        )
    finally:
        scratch.unlink(missing_ok=True)
