"""HTTP client shared by the capture sources: rate limiting, retries, streaming."""

import hashlib
import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import requests

USER_AGENT = "finra-otc/0.2 (+https://github.com/rheineke/finra_otc)"

# Worth retrying: throttling and transient server errors.
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: Mapping[str, str]
    content: bytes = b""

    def header(self, name: str) -> str | None:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None


@dataclass(frozen=True)
class Download:
    """A response streamed to a file instead of held in memory."""

    url: str
    status: int
    headers: Mapping[str, str]
    sha256: str
    size: int

    def header(self, name: str) -> str | None:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None


class HttpClient(Protocol):
    def get(self, url: str) -> HttpResponse: ...

    def head(self, url: str) -> HttpResponse: ...

    def post_json(
        self, url: str, body: Mapping[str, Any], accept: str
    ) -> HttpResponse: ...

    def download(self, url: str, dest: Path) -> Download: ...


class HttpError(RuntimeError):
    def __init__(self, url: str, status: int, body: bytes = b"") -> None:
        detail = body[:300].decode("utf-8", "replace").strip()
        super().__init__(f"HTTP {status} for {url}" + (f": {detail}" if detail else ""))
        self.url = url
        self.status = status


@dataclass
class RequestsClient:
    """requests-based client that spaces out requests and retries transient errors.

    Statuses below 400 are returned, including 204 and 304. Statuses of 400 and
    above raise HttpError, after retries for those in RETRY_STATUSES.
    """

    min_interval: float = 0.1
    timeout: float = 120.0
    retries: int = 5
    backoff: float = 2.0
    session: requests.Session = field(default_factory=requests.Session)
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _last: float | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def get(self, url: str) -> HttpResponse:
        return self._request("GET", url)

    def head(self, url: str) -> HttpResponse:
        return self._request("HEAD", url)

    def post_json(self, url: str, body: Mapping[str, Any], accept: str) -> HttpResponse:
        return self._request(
            "POST",
            url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Accept": accept},
        )

    def download(self, url: str, dest: Path) -> Download:
        """Stream a GET to ``dest``; the caller moves or deletes the file."""
        for attempt in range(self.retries + 1):
            self._wait()
            try:
                with self.session.get(
                    url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=self.timeout,
                    stream=True,
                ) as r:
                    if r.status_code >= 400:
                        if r.status_code in RETRY_STATUSES and attempt < self.retries:
                            self._backoff(attempt, r.headers.get("Retry-After"))
                            continue
                        raise HttpError(r.url, r.status_code, r.content)
                    digest = hashlib.sha256()
                    size = 0
                    with dest.open("wb") as f:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            digest.update(chunk)
                            size += len(chunk)
                            f.write(chunk)
                    expected = r.headers.get("Content-Length")
                    if expected is not None and int(expected) != size:
                        raise requests.ConnectionError(
                            f"{url}: got {size:,} of {int(expected):,} bytes"
                        )
                    return Download(
                        r.url, r.status_code, dict(r.headers), digest.hexdigest(), size
                    )
            except requests.ConnectionError, requests.Timeout:
                dest.unlink(missing_ok=True)
                if attempt == self.retries:
                    raise
                self._backoff(attempt, None)
        raise AssertionError("unreachable")

    def _request(
        self,
        method: str,
        url: str,
        data: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResponse:
        all_headers = {"User-Agent": USER_AGENT, **(headers or {})}
        for attempt in range(self.retries + 1):
            self._wait()
            try:
                r = self.session.request(
                    method,
                    url,
                    data=data,
                    headers=all_headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
            except requests.ConnectionError, requests.Timeout:
                if attempt == self.retries:
                    raise
                self._backoff(attempt, None)
                continue
            if r.status_code in RETRY_STATUSES and attempt < self.retries:
                self._backoff(attempt, r.headers.get("Retry-After"))
                continue
            if r.status_code >= 400:
                raise HttpError(r.url, r.status_code, r.content)
            return HttpResponse(r.url, r.status_code, dict(r.headers), r.content)
        raise AssertionError("unreachable")

    def _backoff(self, attempt: int, retry_after: str | None) -> None:
        delay = self.backoff * 2**attempt
        if retry_after is not None:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                pass
        self.sleep(delay)

    def _wait(self) -> None:
        # Threads share the client, so requests are spaced out across all of them.
        with self._lock:
            now = self.clock()
            if self._last is not None:
                remaining = self.min_interval - (now - self._last)
                if remaining > 0:
                    self.sleep(remaining)
                    now += remaining
            self._last = now
