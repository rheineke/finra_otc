"""Raw store: files saved exactly as received, plus a manifest per source.

Layout under the data directory:

    raw/<source>/<key>/<captured_at>/<file>[.gz]
    raw/<source>/manifest.jsonl     # one line per complete capture of a key
    raw/<source>/absent.jsonl       # keys checked and found missing (CDN only)

A source is a dataset, for example ``api/weeklySummary`` or
``cdn/regsho_daily``. A key is one unit of capture within it: an API partition
or a CDN file. One capture of a key may hold several files, such as the pages
of an API query.

Text is gzip-compressed on disk; gunzipping returns the bytes exactly as
received. Files that are already compressed (ZIPs) are stored as-is. Files are
written first and the manifest line last, so a capture missing from the
manifest is incomplete and is ignored. Nothing is ever overwritten.
"""

import gzip
import hashlib
import json
import os
import re
import shutil
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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

    @staticmethod
    def from_json(line: str) -> Capture:
        d = json.loads(line)
        d["files"] = [StoredFile(**f) for f in d["files"]]
        return Capture(**d)


@dataclass(frozen=True)
class Absence:
    source: str
    key: str
    checked_at: str
    url: str
    status: int


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

    def captures(self, source: str) -> Iterator[Capture]:
        """Every complete capture of a source, oldest first."""
        path = self.manifest_path(source)
        if not path.exists():
            return
        with path.open() as f:
            for line in f:
                # A crash while appending can leave a partial last line.
                if line.endswith("\n") and line.strip():
                    yield Capture.from_json(line)

    def latest(self, source: str) -> dict[str, Capture]:
        """The newest complete capture of each key."""
        latest: dict[str, Capture] = {}
        for capture in self.captures(source):
            latest[capture.key] = capture
        return latest

    def absences(self, source: str) -> dict[str, Absence]:
        path = self.absent_path(source)
        found: dict[str, Absence] = {}
        if not path.exists():
            return found
        with path.open() as f:
            for line in f:
                if line.endswith("\n") and line.strip():
                    a = Absence(**json.loads(line))
                    found[a.key] = a
        return found

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
        """The bytes exactly as received."""
        data = self.path(stored).read_bytes()
        return gzip.decompress(data) if stored.path.endswith(".gz") else data

    def open(self, stored: StoredFile) -> Any:
        """A binary file object over the bytes exactly as received."""
        path = self.path(stored)
        return gzip.open(path, "rb") if stored.path.endswith(".gz") else path.open("rb")

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
        tmp.mkdir(parents=True)
        stored = []
        for name, content, file_meta in files:
            stored_name = check_name(name) + (".gz" if compress else "")
            payload = gzip.compress(content, mtime=0) if compress else content
            (tmp / stored_name).write_bytes(payload)
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
        tmp.mkdir(parents=True)
        size = downloaded.stat().st_size
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
        tmp.rename(folder)
        capture = Capture(
            source=source,
            key=key,
            captured_at=now.astimezone(UTC).isoformat(timespec="microseconds"),
            files=stored,
            meta=dict(meta or {}),
        )
        self._append(self.manifest_path(source), asdict(capture))
        return capture

    @staticmethod
    def _append(path: Path, record: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, sort_keys=True) + "\n"
        # One write call per line, so concurrent appenders don't interleave.
        with path.open("a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
