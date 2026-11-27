import hashlib
from pathlib import Path
from typing import Any

import pytest
import requests

from finra_otc.http import HttpError, RequestsClient


class FakeResponse:
    def __init__(
        self,
        status: int,
        content: bytes = b"ok",
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
    ) -> None:
        self.status_code = status
        self.content = content
        self.url = "https://x"
        self.headers = {
            "Content-Type": "text/plain",
            "Content-Length": str(len(content)),
            **(headers or {}),
        }
        self.chunks = [content] if chunks is None else chunks

    # Streamed, as download() reads it.
    def iter_content(self, chunk_size: int) -> Any:
        yield from self.chunks

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakeSession:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    def request(self, *args: Any, **kwargs: Any) -> FakeResponse:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome  # type: ignore[no-any-return]

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        assert kwargs["stream"] is True
        return self.request("GET", url, **kwargs)


def client(outcomes: list[Any]) -> tuple[RequestsClient, FakeSession, list[float]]:
    session = FakeSession(outcomes)
    sleeps: list[float] = []
    c = RequestsClient(session=session, sleep=sleeps.append, retries=2)  # type: ignore[arg-type]
    return c, session, sleeps


def test_retries_bodies_cut_short_and_throttling() -> None:
    c, session, sleeps = client(
        [
            requests.exceptions.ChunkedEncodingError("cut"),
            FakeResponse(429),
            FakeResponse(200),
        ]
    )
    assert c.get("https://x").content == b"ok"
    assert session.calls == 3 and len(sleeps) >= 2


def test_gives_up_after_retries() -> None:
    c, _, _ = client([requests.ConnectionError()] * 3)
    with pytest.raises(requests.ConnectionError):
        c.get("https://x")


def test_client_errors_are_not_retried() -> None:
    c, session, _ = client([FakeResponse(403, b"<Error/>")])
    with pytest.raises(HttpError) as e:
        c.get("https://x")
    assert e.value.status == 403 and e.value.body == b"<Error/>"
    assert session.calls == 1


def test_a_cancelled_client_abandons_its_retry_wait() -> None:
    from finra_otc.http import Cancelled

    session = FakeSession([FakeResponse(503), FakeResponse(200)])
    c = RequestsClient(session=session, retries=2, backoff=60)  # type: ignore[arg-type]
    c.cancelled.set()
    with pytest.raises(Cancelled):
        c.get("https://x")
    assert session.calls == 1


def test_retry_after_sets_the_wait_up_to_a_cap() -> None:
    session = FakeSession(
        [
            FakeResponse(503, headers={"Retry-After": "30"}),
            FakeResponse(429, headers={"Retry-After": "1000"}),
            FakeResponse(200),
        ]
    )
    sleeps: list[float] = []
    c = RequestsClient(
        session=session,  # type: ignore[arg-type]
        sleep=sleeps.append,
        retries=2,
        backoff=2,
        min_interval=0,
    )
    assert c.get("https://x").content == b"ok"
    assert sleeps == [30.0, 300.0]  # the longer of backoff and Retry-After, capped


def test_download_streams_to_the_file(tmp_path: Path) -> None:
    body = b"abc" * 10
    c, _, _ = client([FakeResponse(200, body, chunks=[body[:7], body[7:]])])
    dest = tmp_path / "f.zip"
    d = c.download("https://x", dest)
    assert (d.size, d.sha256) == (30, hashlib.sha256(body).hexdigest())
    assert dest.read_bytes() == body


def test_download_retries_bodies_cut_short_and_throttling(tmp_path: Path) -> None:
    short = FakeResponse(200, b"abcdef", chunks=[b"abc"])  # Content-Length 6
    c, session, _ = client(
        [short, requests.ConnectionError("reset"), FakeResponse(503), FakeResponse(200)]
    )
    c.retries = 3
    dest = tmp_path / "f.zip"
    assert c.download("https://x", dest).size == 2
    assert session.calls == 4 and dest.read_bytes() == b"ok"


def test_download_gives_up_and_removes_the_partial_file(tmp_path: Path) -> None:
    c, session, _ = client([FakeResponse(200, b"abcdef", chunks=[b"abc"])] * 3)
    dest = tmp_path / "f.zip"
    with pytest.raises(requests.ConnectionError, match="got 3 of 6 bytes"):
        c.download("https://x", dest)
    assert session.calls == 3 and not dest.exists()


def test_download_skips_the_length_check_for_encoded_bodies(tmp_path: Path) -> None:
    # Content-Length then counts the compressed bytes, not what was written.
    encoded = FakeResponse(200, b"abcdef", {"Content-Encoding": "gzip"})
    encoded.headers["Content-Length"] = "4"
    c, _, _ = client([encoded])
    assert c.download("https://x", tmp_path / "f").size == 6
