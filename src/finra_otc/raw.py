"""Raw store: files saved exactly as received, plus a manifest per source.

Layout under the data directory:

    raw/<source>/<key>/<captured_at>/<file>[.gz]
    raw/<source>/manifest.jsonl     # one line per complete capture of a key
    raw/<source>/absent.jsonl       # keys checked and found missing (CDN only)
    raw/<source>/checks.jsonl       # keys fetched again and found unchanged

A source is a dataset, for example ``api/weeklySummary`` or
``cdn/regsho_daily``. A key is one unit of capture within it: an API partition
or a CDN file. One capture of a key may hold several files, such as the pages
of an API query.

Text is gzip-compressed on disk; gunzipping returns the bytes exactly as
received. Files that are already compressed (ZIPs) are stored as-is. Files are
written and synced to disk (F_FULLFSYNC on macOS) first and the manifest line
last, so a capture missing from the manifest is incomplete and is ignored.
Reads check each file against the SHA-256 in the manifest. Nothing is ever
overwritten.
"""

import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import time
import warnings
import zlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

DATA_DIR_ENV = "FINRA_OTC_DATA"

# Keys and file names become path components.
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]*")


def data_dir() -> Path:
    return Path(os.environ.get(DATA_DIR_ENV, "data"))


def check_name(name: str) -> str:
    if not _SAFE_NAME.fullmatch(name) or ".." in name:
        raise ValueError(f"unsafe name for a raw file or key: {name!r}")
    return name


@dataclass(frozen=True)
class StoredFile:
    name: str  # as received, without the .gz suffix
    path: str  # relative to raw/
    sha256: str  # of the bytes as received
    received_bytes: int
    stored_bytes: int
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Capture:
    source: str
    key: str
    captured_at: str  # UTC, ISO 8601 to the microsecond
    files: list[StoredFile]
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def sha256s(self) -> list[str]:
        return [f.sha256 for f in self.files]

    @staticmethod
    def sha_of(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()


@dataclass(frozen=True)
class Absence:
    source: str
    key: str
    checked_at: str
    url: str
    status: int


class CorruptFile(OSError):
    """A raw file doesn't match its manifest entry."""


def _stamp(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")


class RawStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    def source_dir(self, source: str) -> Path:
        for part in source.split("/"):
            check_name(part)
        return self.raw_dir / source

    def manifest_path(self, source: str) -> Path:
        return self.source_dir(source) / "manifest.jsonl"

    def absent_path(self, source: str) -> Path:
        return self.source_dir(source) / "absent.jsonl"

    def checks_path(self, source: str) -> Path:
        return self.source_dir(source) / "checks.jsonl"

    @staticmethod
    def _records(path: Path) -> Iterator[dict[str, Any]]:
        if not path.exists():
            return
        with path.open() as f:
            for number, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # A crash while appending leaves a partial line.
                    warnings.warn(
                        f"{path}:{number}: skipping a damaged line", stacklevel=2
                    )

    def captures(self, source: str) -> Iterator[Capture]:
        """Every complete capture of a source, in the order recorded."""
        for record in self._records(self.manifest_path(source)):
            record["files"] = [StoredFile(**f) for f in record["files"]]
            yield Capture(**record)

    def latest(self, source: str) -> dict[str, Capture]:
        """The newest complete capture of each key, by capture time."""
        latest: dict[str, Capture] = {}
        for capture in self.captures(source):
            previous = latest.get(capture.key)
            if previous is None or capture.captured_at >= previous.captured_at:
                latest[capture.key] = capture
        return latest

    def absences(self, source: str) -> dict[str, Absence]:
        found: dict[str, Absence] = {}
        for record in self._records(self.absent_path(source)):
            a = Absence(**record)
            found[a.key] = a
        return found

    def checks(self, source: str) -> dict[str, dict[str, Any]]:
        """The newest unchanged re-fetch of each key."""
        found: dict[str, dict[str, Any]] = {}
        for record in self._records(self.checks_path(source)):
            found[record["key"]] = record
        return found

    def record_check(self, source: str, key: str, now: datetime, **fields: Any) -> None:
        record = {"key": check_name(key), "checked_at": now.isoformat(), **fields}
        self._append(self.checks_path(source), record)

    def record_absence(
        self, source: str, key: str, url: str, status: int, now: datetime
    ) -> None:
        a = Absence(source, check_name(key), now.isoformat(), url, status)
        self._append(self.absent_path(source), asdict(a))

    def sources(self) -> list[str]:
        if not self.raw_dir.exists():
            return []
        return sorted(
            p.parent.relative_to(self.raw_dir).as_posix()
            for p in self.raw_dir.glob("**/manifest.jsonl")
        )

    def path(self, stored: StoredFile) -> Path:
        return self.raw_dir / stored.path

    def read(self, stored: StoredFile) -> bytes:
        """The bytes exactly as received, checked against the manifest."""
        try:
            data = self.path(stored).read_bytes()
            if stored.path.endswith(".gz"):
                data = gzip.decompress(data)
        except (EOFError, zlib.error, gzip.BadGzipFile) as e:
            raise CorruptFile(f"{stored.path}: {e}") from e
        if hashlib.sha256(data).hexdigest() != stored.sha256:
            raise CorruptFile(f"{stored.path}: SHA-256 doesn't match the manifest")
        return data

    def verify(self, stored: StoredFile) -> None:
        """Check a file stored as-is against the manifest, without loading it."""
        digest = hashlib.sha256()
        with self.path(stored).open("rb") as f:
            while chunk := f.read(1 << 24):
                digest.update(chunk)
        if digest.hexdigest() != stored.sha256:
            raise CorruptFile(f"{stored.path}: SHA-256 doesn't match the manifest")

    def capture_dir(self, source: str, key: str, now: datetime) -> Path:
        return self.source_dir(source) / check_name(key) / _stamp(now)

    def save(
        self,
        source: str,
        key: str,
        files: Sequence[tuple[str, bytes, Mapping[str, Any]]],
        *,
        meta: Mapping[str, Any] | None = None,
        compress: bool = True,
        now: datetime | None = None,
    ) -> Capture:
        """Save one capture of ``key``: (name, bytes as received, meta) per file."""
        now = now or datetime.now(UTC)
        folder = self.capture_dir(source, key, now)
        tmp = folder.with_name(folder.name + ".partial")
        if folder.exists():
            raise FileExistsError(folder)
        shutil.rmtree(tmp, ignore_errors=True)
        _make_dirs(tmp)
        stored = []
        for name, content, file_meta in files:
            stored_name = check_name(name) + (".gz" if compress else "")
            payload = gzip.compress(content, mtime=0) if compress else content
            _write_synced(tmp / stored_name, payload)
            stored.append(
                StoredFile(
                    name=name,
                    path=(folder / stored_name).relative_to(self.raw_dir).as_posix(),
                    sha256=hashlib.sha256(content).hexdigest(),
                    received_bytes=len(content),
                    stored_bytes=len(payload),
                    meta=dict(file_meta),
                )
            )
        return self._commit(source, key, tmp, folder, stored, meta, now)

    def save_download(
        self,
        source: str,
        key: str,
        name: str,
        downloaded: Path,
        sha256: str,
        *,
        meta: Mapping[str, Any] | None = None,
        file_meta: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> Capture:
        """Move a file already streamed to disk (stored as-is) into the store."""
        now = now or datetime.now(UTC)
        folder = self.capture_dir(source, key, now)
        tmp = folder.with_name(folder.name + ".partial")
        if folder.exists():
            raise FileExistsError(folder)
        shutil.rmtree(tmp, ignore_errors=True)
        _make_dirs(tmp)
        size = downloaded.stat().st_size
        with downloaded.open("rb+") as f:
            _fsync(f.fileno())
        downloaded.replace(tmp / check_name(name))
        stored = [
            StoredFile(
                name=name,
                path=(folder / name).relative_to(self.raw_dir).as_posix(),
                sha256=sha256,
                received_bytes=size,
                stored_bytes=size,
                meta=dict(file_meta or {}),
            )
        ]
        return self._commit(source, key, tmp, folder, stored, meta, now)

    def sweep(self, source: str, older_than: timedelta = timedelta(days=1)) -> int:
        """Remove leftovers of interrupted captures; return how many.

        Unfinished capture folders (*.partial) and downloads (.scratch/*) that
        are older than ``older_than``, so that a running capture keeps its own.
        """
        root = self.source_dir(source)
        if not root.exists():
            return 0
        cutoff = time.time() - older_than.total_seconds()
        stale = [p for p in root.glob("*/*.partial") if p.stat().st_mtime < cutoff]
        stale += [p for p in root.glob(".scratch/*") if p.stat().st_mtime < cutoff]
        for p in stale:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
        return len(stale)

    def scratch_dir(self, source: str) -> Path:
        """Where in-progress downloads go, on the same filesystem as the store."""
        path = self.source_dir(source) / ".scratch"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _commit(
        self,
        source: str,
        key: str,
        tmp: Path,
        folder: Path,
        stored: list[StoredFile],
        meta: Mapping[str, Any] | None,
        now: datetime,
    ) -> Capture:
        _sync_dir(tmp)
        tmp.rename(folder)
        _sync_dir(folder.parent)
        capture = Capture(
            source=source,
            key=key,
            captured_at=now.astimezone(UTC).isoformat(timespec="microseconds"),
            files=stored,
            meta=dict(meta or {}),
        )
        self._append(self.manifest_path(source), asdict(capture))
        return capture

    def _append(self, path: Path, record: Mapping[str, Any]) -> None:
        _make_dirs(path.parent)
        new = not path.exists()
        line = json.dumps(record, sort_keys=True) + "\n"
        with path.open("ab") as f:
            # End a line left partial by a crash, so this one stays readable.
            if f.tell() > 0:
                with path.open("rb") as r:
                    r.seek(-1, os.SEEK_END)
                    if r.read(1) != b"\n":
                        line = "\n" + line
            # One write call per line, so concurrent appenders don't interleave.
            f.write(line.encode())
            f.flush()
            _fsync(f.fileno())
        if new:
            _sync_dir(path.parent)


def _fsync(fd: int) -> None:
    # On macOS fsync leaves data in the drive's cache; F_FULLFSYNC flushes it.
    if sys.platform == "darwin":
        try:
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
            return
        except OSError:
            pass
    os.fsync(fd)


def _write_synced(path: Path, payload: bytes) -> None:
    with path.open("wb") as f:
        f.write(payload)
        f.flush()
        _fsync(f.fileno())


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        _fsync(fd)
    finally:
        os.close(fd)


def _make_dirs(path: Path) -> None:
    """mkdir -p, syncing each parent whose listing gained a new directory."""
    missing = []
    while not path.exists() and path.parent != path:
        missing.append(path)
        path = path.parent
    for d in reversed(missing):
        d.mkdir(exist_ok=True)
        _sync_dir(d.parent)
