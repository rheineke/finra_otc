from typing import Any

import pytest
import requests

from finra_otc.http import HttpError, RequestsClient


class FakeResponse:
    def __init__(self, status: int, content: bytes = b"ok") -> None:
        self.status_code = status
        self.content = content
        self.url = "https://x"
        self.headers = {"Content-Type": "text/plain"}


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
