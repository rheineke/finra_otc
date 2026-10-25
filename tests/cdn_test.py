import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

from fakes import FakeClient, response

from finra_otc import cdn
from finra_otc.raw import RawStore

DAILY = b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n" + (
    b"20261022|A|10|0|20|Q\n1\n"
)


def at(day: date, hour: int = 22) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def test_weekdays_and_months() -> None:
    days = list(cdn.weekdays(date(2026, 10, 23), date(2026, 10, 26)))
    assert days == [date(2026, 10, 23), date(2026, 10, 26)]
    assert list(cdn.months(date(2025, 11, 15), date(2026, 2, 1))) == [
        date(2025, 11, 1),
        date(2025, 12, 1),
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_urls() -> None:
    assert cdn.daily_url("CNMS", date(2026, 10, 23)) == (
        "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20261023.txt"
    )
    assert cdn.monthly_url("FNSQ", date(2026, 9, 1), 2) == (
        "https://cdn.finra.org/equity/regsho/monthly/FNSQsh202609_2.zip"
    )


def test_capture_daily_saves_and_records_absences(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    url = cdn.daily_url("FORF", date(2026, 10, 22))
    client = FakeClient(routes={url: response(url, DAILY, headers={"ETag": '"e1"'})})
    today = date(2026, 10, 23)
    r = cdn.capture_daily(
        client,
        store,
        since=date(2026, 10, 22),
        facilities=["FORF"],
        now=lambda: at(today),
    )
    assert (r.saved, r.absent) == (1, 1)  # the 23rd isn't posted yet
    capture = store.latest(cdn.DAILY_SOURCE)["FORFshvol20261022"]
    assert store.read(capture.files[0]) == DAILY
    assert capture.files[0].meta["etag"] == '"e1"'
    assert set(store.absences(cdn.DAILY_SOURCE)) == {"FORFshvol20261023"}


def test_capture_daily_rechecks_recent_files_by_etag(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    day = date(2026, 10, 22)
    url = cdn.daily_url("FORF", day)
    client = FakeClient(routes={url: response(url, DAILY, headers={"ETag": '"e1"'})})
    kw = dict(since=day, until=day, facilities=["FORF"])
    cdn.capture_daily(client, store, now=lambda: at(day), **kw)  # type: ignore[arg-type]
    client.calls.clear()
    r = cdn.capture_daily(client, store, now=lambda: at(date(2026, 10, 23)), **kw)  # type: ignore[arg-type]
    assert r.unchanged == 1 and client.calls == [("HEAD", url)]

    updated = DAILY.replace(b"|10|", b"|11|")
    client.routes[url] = response(url, updated, headers={"ETag": '"e2"'})
    r = cdn.capture_daily(client, store, now=lambda: at(date(2026, 10, 24)), **kw)  # type: ignore[arg-type]
    assert r.saved == 1
    assert len(list(store.captures(cdn.DAILY_SOURCE))) == 2

    # Old files aren't checked again.
    client.calls.clear()
    r = cdn.capture_daily(client, store, now=lambda: at(date(2026, 12, 1)), **kw)  # type: ignore[arg-type]
    assert r.skipped == 1 and client.calls == []


def test_capture_daily_skips_settled_absences(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    day = date(2026, 7, 3)  # a holiday
    client = FakeClient()
    kw = dict(since=day, until=day, facilities=["FORF"])
    # Missing on the day itself: asked again next time.
    cdn.capture_daily(client, store, now=lambda: at(day), **kw)  # type: ignore[arg-type]
    r = cdn.capture_daily(client, store, now=lambda: at(date(2026, 8, 1)), **kw)  # type: ignore[arg-type]
    assert r.checked == 1
    # Missing well after it was due: not asked again.
    r = cdn.capture_daily(client, store, now=lambda: at(date(2026, 8, 2)), **kw)  # type: ignore[arg-type]
    assert (r.checked, r.skipped) == (0, 1)
    r = cdn.capture_daily(
        client,
        store,
        recheck_absent=True,
        now=lambda: at(date(2026, 8, 2)),
        **kw,  # type: ignore[arg-type]
    )
    assert r.checked == 1


def zipped(name: str, text: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, text)
    return buf.getvalue()


def test_capture_monthly_finds_parts(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    month = date(2026, 9, 1)
    routes = {}
    for part in (1, 2):
        url = cdn.monthly_url("FNSQ", month, part)
        routes[url] = response(
            url, zipped("x.txt", b"data"), headers={"ETag": f'"{part}"'}
        )
    client = FakeClient(routes=routes)
    now = lambda: at(date(2026, 10, 24))  # noqa: E731
    r = cdn.capture_monthly(
        client, store, since=month, facilities=["FNSQ"], dry_run=True, now=now
    )
    assert r.saved == 0 and r.bytes > 0 and store.latest(cdn.MONTHLY_SOURCE) == {}
    r = cdn.capture_monthly(client, store, since=month, facilities=["FNSQ"], now=now)
    assert r.saved == 2
    assert set(store.latest(cdn.MONTHLY_SOURCE)) == {"FNSQsh202609_1", "FNSQsh202609_2"}
    assert not any(store.scratch_dir(cdn.MONTHLY_SOURCE).iterdir())
    # Unchanged ETags: nothing downloaded again.
    client.calls.clear()
    r = cdn.capture_monthly(client, store, since=month, facilities=["FNSQ"], now=now)
    assert r.unchanged == 2 and not any(m == "GET" for m, _ in client.calls)


def test_capture_monthly_rejects_non_zip(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    month = date(2026, 9, 1)
    url = cdn.monthly_url("FNYX", month, None)
    client = FakeClient(routes={url: response(url, b"<html>")})
    r = cdn.capture_monthly(
        client,
        store,
        since=month,
        facilities=["FNYX"],
        now=lambda: at(date(2026, 10, 24)),
    )
    assert r.failed == 1 and store.latest(cdn.MONTHLY_SOURCE) == {}


def test_a_block_page_is_a_failure_not_a_missing_file(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    day = date(2026, 10, 22)
    url = cdn.daily_url("FORF", day)
    block = response(url, b"<html>Attention Required</html>", status=403,
                     headers={"Content-Type": "text/html"})  # fmt: skip
    client = FakeClient(routes={url: block})
    r = cdn.capture_daily(
        client, store, since=day, until=day, facilities=["FORF"], now=lambda: at(day)
    )
    assert (r.failed, r.absent) == (1, 0)
    assert store.absences(cdn.DAILY_SOURCE) == {}


def test_recent_monthly_without_etag_uses_last_modified(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    month = date(2026, 9, 1)
    url = cdn.monthly_url("FNYX", month, None)
    headers = {"Last-Modified": "Thu, 08 Oct 2026 22:34:32 GMT"}
    client = FakeClient(
        routes={url: response(url, zipped("x.txt", b"d"), headers=headers)}
    )
    days = iter(range(20, 30))

    def run() -> cdn.Result:
        day = date(2026, 10, next(days))
        return cdn.capture_monthly(
            client, store, since=month, facilities=["FNYX"], now=lambda: at(day)
        )

    assert run().saved == 1
    assert run().unchanged == 1
    headers = {"Last-Modified": "Fri, 09 Oct 2026 10:00:00 GMT"}
    client.routes[url] = response(url, zipped("x.txt", b"e"), headers=headers)
    assert run().saved == 1


def test_a_daily_file_cut_short_is_not_saved(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    day = date(2026, 10, 22)
    url = cdn.daily_url("FORF", day)
    cut = DAILY.replace(b"\n1\n", b"\n")
    client = FakeClient(routes={url: response(url, cut)})
    r = cdn.capture_daily(
        client, store, since=day, until=day, facilities=["FORF"], now=lambda: at(day)
    )
    assert r.failed == 1 and "footer" in r.errors[0]
    assert store.latest(cdn.DAILY_SOURCE) == {}


def test_daily_problem_accepts_crlf_and_empty_files() -> None:
    assert cdn.daily_problem(DAILY.replace(b"\n", b"\r\n")) is None
    empty = b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n"
    assert cdn.daily_problem(empty) is None


def test_a_corrupt_zip_fails_one_file(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    month = date(2026, 9, 1)
    good = zipped("x.txt", b"data" * 1000)
    # Damage the deflate stream but keep the ZIP structure.
    bad = good[:40] + bytes(b ^ 0xFF for b in good[40:60]) + good[60:]
    urls = [cdn.monthly_url(f, month, None) for f in ("FNYX", "FNRA")]
    client = FakeClient(
        routes={urls[0]: response(urls[0], bad), urls[1]: response(urls[1], good)}
    )
    r = cdn.capture_monthly(
        client, store, since=month, facilities=["FNYX", "FNRA"],
        now=lambda: at(date(2026, 10, 24)),
    )  # fmt: skip
    assert (r.failed, r.saved) == (1, 1)
