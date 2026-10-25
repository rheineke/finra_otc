import gzip
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from finra_otc.raw import CorruptFile, RawStore, check_name

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
    with pytest.warns(UserWarning):
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
    sha = hashlib.sha256(b"PKdata").hexdigest()
    capture = store.save_download("s", "k", "f.zip", scratch, sha, now=NOW)
    assert not scratch.exists()
    assert store.read(capture.files[0]) == b"PKdata"
    store.verify(capture.files[0])


def test_damaged_files_are_detected(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    capture = store.save("s", "k", [("a", b"x" * 1000, {})], now=NOW)
    path = store.path(capture.files[0])
    path.write_bytes(path.read_bytes()[:20])  # truncated gzip
    with pytest.raises(CorruptFile):
        store.read(capture.files[0])
    path.write_bytes(gzip.compress(b"y" * 1000))  # wrong content
    with pytest.raises(CorruptFile):
        store.read(capture.files[0])


def test_append_after_a_partial_line_keeps_the_manifest_readable(
    tmp_path: Path,
) -> None:
    store = RawStore(tmp_path)
    store.save("s", "k1", [("f", b"a", {})], now=NOW)
    with store.manifest_path("s").open("a") as f:
        f.write('{"source": "s", "key": "broken"')  # crash mid-append
    store.save("s", "k2", [("f", b"b", {})], now=NOW.replace(second=1))
    with pytest.warns(UserWarning, match="damaged line"):
        assert set(store.latest("s")) == {"k1", "k2"}


def test_latest_is_by_capture_time_not_manifest_order(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    newer = store.save("s", "k", [("f", b"new", {})], now=NOW.replace(second=5))
    store.save("s", "k", [("f", b"old", {})], now=NOW)  # a slower process
    assert store.latest("s")["k"] == newer


def test_checks(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.record_check("s", "k", NOW, reason="recent")
    assert store.checks("s")["k"]["checked_at"] == NOW.isoformat()


def test_absences(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    store.record_absence("s", "k", "https://x", 403, NOW)
    assert store.absences("s")["k"].status == 403


def test_sweep_removes_old_leftovers_only(tmp_path: Path) -> None:
    import os
    import time

    store = RawStore(tmp_path)
    store.save("s", "k", [("f", b"a", {})], now=NOW)
    old = store.source_dir("s") / "k" / "x.partial"
    old.mkdir()
    part = store.scratch_dir("s") / "f.part"
    part.write_bytes(b"x")
    fresh = store.scratch_dir("s") / "g.part"
    fresh.write_bytes(b"x")
    two_days_ago = time.time() - 2 * 86400
    for p in (old, part):
        os.utime(p, (two_days_ago, two_days_ago))
    assert store.sweep("s") == 2
    assert not old.exists() and not part.exists() and fresh.exists()
    assert store.latest("s")


def test_sweep_removes_old_unrecorded_capture_folders(tmp_path: Path) -> None:
    import os
    import time

    store = RawStore(tmp_path)
    kept = store.save("s", "k", [("f", b"a", {})], now=NOW)
    orphan = store.source_dir("s") / "k" / "20260101T000000000000Z"
    orphan.mkdir()
    (orphan / "f.gz").write_bytes(b"x")
    two_days_ago = time.time() - 2 * 86400
    os.utime(orphan, (two_days_ago, two_days_ago))
    os.utime(store.path(kept.files[0]).parent, (two_days_ago, two_days_ago))
    assert store.sweep("s") == 1
    assert not orphan.exists() and store.read(kept.files[0]) == b"a"
