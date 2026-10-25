from datetime import UTC, datetime
from pathlib import Path

import pytest

from finra_otc.raw import RawStore, check_name

NOW = datetime(2026, 10, 24, 12, 0, 0, 123456, tzinfo=UTC)


def test_save_and_read_round_trip(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    capture = store.save(
        "api/x",
        "2026-10-19_T1",
        [("a.csv", b"one", {"rows": 1}), ("b.csv", b"two", {})],
        meta={"m": 1},
        now=NOW,
    )
    assert [store.read(f) for f in capture.files] == [b"one", b"two"]
    assert capture.files[0].path == (
        "api/x/2026-10-19_T1/20261024T120000123456Z/a.csv.gz"
    )
    assert capture.captured_at == "2026-10-24T12:00:00.123456+00:00"
    assert store.latest("api/x") == {"2026-10-19_T1": capture}
    assert store.sources() == ["api/x"]


def test_uncompressed_files_are_stored_as_is(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    capture = store.save("s", "k", [("f.zip", b"PK..", {})], compress=False, now=NOW)
    assert store.path(capture.files[0]).read_bytes() == b"PK.."


def test_latest_is_newest_capture(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.save("s", "k", [("f", b"old", {})], now=NOW)
    later = NOW.replace(second=1)
    newer = store.save("s", "k", [("f", b"new", {})], now=later)
    assert store.latest("s")["k"] == newer
    assert len(list(store.captures("s"))) == 2


def test_never_overwrites(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.save("s", "k", [("f", b"a", {})], now=NOW)
    with pytest.raises(FileExistsError):
        store.save("s", "k", [("f", b"b", {})], now=NOW)


def test_partial_manifest_line_is_ignored(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.save("s", "k", [("f", b"a", {})], now=NOW)
    with store.manifest_path("s").open("a") as f:
        f.write('{"source": "s", "key": "k2"')  # crash mid-append
    assert list(store.latest("s")) == ["k"]


def test_capture_without_manifest_line_is_invisible(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    (store.source_dir("s") / "k" / "x.partial").mkdir(parents=True)
    assert store.latest("s") == {}


@pytest.mark.parametrize("name", ["", "../x", "a/b", "a b", ".hidden", "a..b"])
def test_unsafe_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError):
        check_name(name)


def test_save_download_moves_file(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    scratch = store.scratch_dir("s") / "f.part"
    scratch.write_bytes(b"PKdata")
    capture = store.save_download("s", "k", "f.zip", scratch, "abc", now=NOW)
    assert not scratch.exists()
    assert store.read(capture.files[0]) == b"PKdata"
    assert capture.files[0].sha256 == "abc"


def test_absences(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.record_absence("s", "k", "https://x", 403, NOW)
    assert store.absences("s")["k"].status == 403
