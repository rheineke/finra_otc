"""Test doubles for the HTTP client."""

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from finra_otc.http import Download, HttpError, HttpResponse

MISSING_BODY = (
    b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
)

Handler = Callable[[Mapping[str, Any] | None], HttpResponse]


def response(
    url: str,
    content: bytes = b"",
    status: int = 200,
    headers: Mapping[str, str] | None = None,
) -> HttpResponse:
    return HttpResponse(url, status, dict(headers or {}), content)


@dataclass
class FakeClient:
    """Answers GET and HEAD from ``routes``, POST from ``posts``; else 403."""

    routes: dict[str, HttpResponse] = field(default_factory=dict)
    posts: dict[str, Handler] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)

    def _route(self, method: str, url: str) -> HttpResponse:
        self.calls.append((method, url))
        r = self.routes.get(url)
        if r is None:
            # What the CDN's bucket answers for a missing file.
            body = b"" if method == "HEAD" else MISSING_BODY
            raise HttpError(url, 403, body, {"Content-Type": "application/xml"})
        if r.status >= 400:
            raise HttpError(url, r.status, r.content, r.headers)
        return r

    def get(self, url: str) -> HttpResponse:
        return self._route("GET", url)

    def head(self, url: str) -> HttpResponse:
        r = self._route("HEAD", url)
        headers = {**r.headers, "Content-Length": str(len(r.content))}
        return HttpResponse(r.url, r.status, headers, b"")

    def post_json(self, url: str, body: Mapping[str, Any], accept: str) -> HttpResponse:
        self.calls.append(("POST", url))
        return self.posts[url](body)

    def download(self, url: str, dest: Path) -> Download:
        r = self._route("GET", url)
        dest.write_bytes(r.content)
        return Download(
            r.url,
            r.status,
            r.headers,
            hashlib.sha256(r.content).hexdigest(),
            len(r.content),
        )
