"""The shared reconnect policy: permanent errors stop, transient ones back off,
and only consecutive failed connections count against ``max_attempts``."""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

from hot import AsyncHotClient, HotApiError, HotClient, HotKeyScopeError, _streaming

PUBLISHED = {"type": "event:published", "stream_id": "s1", "event_id": "e1"}
COMPLETE = {"type": "stream:complete", "stream_id": "s1"}


def sse(*events: dict) -> bytes:
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events).encode()


def run(status: str, run_id: str = "r1", **fields) -> dict:
    return {"run_id": run_id, "event_id": "e1", "status": status, "result": "done", **fields}


def run_update(status: str) -> dict:
    return {"type": "run:update", "run": run(status)}


def task_update(status: str) -> dict:
    return {"type": "task:update", "task": {"task_id": "t1", "status": status, "result": 1}}


def stop(**fields) -> dict:
    return {"type": "run:stop", "run": run("succeeded", **fields)}


def error(status: int, retry_after: str | None = None) -> httpx.Response:
    headers = {"Retry-After": retry_after} if retry_after else {}
    code = "key_scope_mismatch" if status == 403 else "error"
    return httpx.Response(
        status, json={"error": {"code": code, "message": f"status {status}"}}, headers=headers
    )


def ok(*events: dict) -> httpx.Response:
    return httpx.Response(200, content=sse(*events), headers={"content-type": "text/event-stream"})


class Script:
    """Answer requests from a list; an exception instance is raised instead."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if not self.responses:
            raise AssertionError(f"unexpected request {request.method} {request.url.path}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture
def sleeps(monkeypatch):
    delays: list[float] = []

    async def asleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(_streaming, "sleep", delays.append)
    monkeypatch.setattr(_streaming, "asleep", asleep)
    return delays


def client(script: Script) -> HotClient:
    return HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(script)),
    )


def aclient(script: Script) -> AsyncHotClient:
    return AsyncHotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.AsyncClient(transport=httpx.MockTransport(script)),
    )


WAITERS = {
    "runs.wait": lambda hot, **kw: hot.runs.wait("r1", **kw),
    "tasks.wait": lambda hot, **kw: hot.tasks.wait("t1", **kw),
    "wait_for_run_result": lambda hot, **kw: hot.streams.wait_for_run_result("s1", "e1", **kw),
}
SUCCESS = {
    "runs.wait": ok(run_update("succeeded")),
    "tasks.wait": ok(task_update("completed")),
    "wait_for_run_result": ok(stop()),
}


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
@pytest.mark.parametrize("waiter", list(WAITERS))
def test_permanent_errors_stop_after_one_request(waiter, status, sleeps):
    script = Script(error(status))
    with client(script) as hot:
        with pytest.raises(HotApiError) as raised:
            WAITERS[waiter](hot)
    assert raised.value.status_code == status
    assert len(script.calls) == 1
    assert sleeps == []


@pytest.mark.parametrize("waiter", list(WAITERS))
def test_transient_errors_back_off_then_succeed(waiter, sleeps):
    script = Script(
        error(503),
        error(502),
        httpx.ConnectError("refused"),
        error(408),
        SUCCESS[waiter],
    )
    with client(script) as hot:
        WAITERS[waiter](hot)
    assert len(script.calls) == 5
    assert len(sleeps) == 4
    # Jittered exponential backoff: d/2 <= sleep <= d for d = 0.5, 1, 2, 4.
    for n, delay in enumerate(sleeps):
        ceiling = 0.5 * 2**n
        assert ceiling / 2 <= delay <= ceiling


def test_retry_after_is_honoured(sleeps):
    script = Script(error(429, retry_after="3"), SUCCESS["runs.wait"])
    with client(script) as hot:
        hot.runs.wait("r1")
    assert sleeps[0] >= 3


def test_consecutive_failures_exhaust_max_attempts(sleeps):
    script = Script(*[error(503) for _ in range(3)])
    with client(script) as hot:
        with pytest.raises(HotApiError) as raised:
            hot.runs.wait("r1", max_attempts=2)
    assert raised.value.status_code == 503
    assert len(script.calls) == 3


def test_backoff_never_sleeps_past_the_deadline(sleeps):
    script = Script(error(503, retry_after="30"))
    started = time.monotonic()
    with client(script) as hot:
        with pytest.raises(TimeoutError) as raised:
            hot.runs.wait("r1", timeout=5)
    assert time.monotonic() - started < 1
    assert sleeps == []
    assert isinstance(raised.value.__cause__, HotApiError)


@pytest.mark.parametrize("waiter", list(WAITERS))
def test_rollovers_do_not_count_against_max_attempts(waiter, sleeps):
    pending = {
        "runs.wait": run_update("running"),
        "tasks.wait": task_update("running"),
        "wait_for_run_result": {"type": "run:start", "run": run("running")},
    }[waiter]
    rollovers = [ok(pending, COMPLETE) for _ in range(8)]
    script = Script(*rollovers, SUCCESS[waiter])
    with client(script) as hot:
        WAITERS[waiter](hot, max_attempts=2)
    assert len(script.calls) == 9
    assert sleeps == [0.0] * 8


def test_clean_end_without_events_counts_as_failure(sleeps):
    script = Script(ok(), ok(), ok())
    with client(script) as hot:
        with pytest.raises(RuntimeError, match="Stream ended before run completed"):
            hot.streams.wait_for_run_result("s1", "e1", max_attempts=2)
    assert len(script.calls) == 3
    assert len(sleeps) == 2 and all(delay > 0 for delay in sleeps)


def test_failure_count_resets_after_a_healthy_connection(sleeps):
    healthy = ok({"type": "run:start", "run": run("running")}, COMPLETE)
    script = Script(error(503), error(503), healthy, error(503), error(503), ok(stop()))
    with client(script) as hot:
        hot.streams.wait_for_run_result("s1", "e1", max_attempts=2)
    assert len(script.calls) == 6


def test_subscribe_with_event_stops_on_permanent_reconnect_error(sleeps):
    script = Script(ok(PUBLISHED), error(403))
    with client(script) as hot:
        with pytest.raises(HotKeyScopeError):
            list(hot.streams.subscribe_with_event({"event_type": "x", "event_data": {}}))
    assert script.calls == [
        ("POST", "/v1/streams/subscribe-with-event"),
        ("GET", "/v1/streams/s1/subscribe"),
    ]


def test_subscribe_with_event_retries_transient_reconnect_errors(sleeps):
    script = Script(ok(PUBLISHED), error(503), error(429, "1"), ok(stop()))
    with client(script) as hot:
        events = list(hot.streams.subscribe_with_event({"event_type": "x", "event_data": {}}))
    assert [event["type"] for event in events] == ["event:published", "run:stop"]
    # The first connection delivered event:published, so it reconnects at once.
    assert sleeps[0] == 0.0 and len(sleeps) == 3 and sleeps[2] >= 1


def test_subscribe_with_event_survives_more_rollovers_than_max_attempts(sleeps):
    start = {"type": "run:start", "run": run("running")}
    rollovers = [ok(COMPLETE) for _ in range(10)]
    script = Script(ok(PUBLISHED, start, COMPLETE), *rollovers, ok(stop()))
    with client(script) as hot:
        events = list(
            hot.streams.subscribe_with_event({"event_type": "x", "event_data": {}}, max_attempts=1)
        )
    assert events[-1]["type"] == "run:stop"
    assert len(script.calls) == 12


def test_subscribe_with_event_timeout_bounds_the_subscription(sleeps):
    script = Script(ok(PUBLISHED), error(503, retry_after="60"))
    with client(script) as hot:
        with pytest.raises(TimeoutError):
            list(hot.streams.subscribe_with_event({"event_type": "x", "event_data": {}}, timeout=5))


def test_publish_is_retried_only_when_the_server_rejected_it(sleeps):
    script = Script(error(503), ok(PUBLISHED, stop()))
    with client(script) as hot:
        assert hot.events.call_hot("::f/g") == "done"
    assert len(script.calls) == 2

    # A transport error may have published the event, so it is not retried.
    script = Script(httpx.ReadError("reset"))
    with client(script) as hot:
        with pytest.raises(httpx.ReadError):
            hot.events.call_hot("::f/g")
    assert len(script.calls) == 1


def test_call_hot_reconnects_across_rollovers(sleeps):
    script = Script(ok(PUBLISHED, COMPLETE), *[ok(COMPLETE) for _ in range(7)], ok(stop()))
    with client(script) as hot:
        assert hot.events.call_hot("::f/g") == "done"
    assert script.calls[0] == ("POST", "/v1/streams/subscribe-with-event")
    assert set(script.calls[1:]) == {("GET", "/v1/streams/s1/subscribe")}


def test_async_waiters_follow_the_same_policy(sleeps):
    async def scenario():
        script = Script(error(401))
        async with aclient(script) as hot:
            with pytest.raises(HotApiError):
                await hot.tasks.wait("t1")
        assert len(script.calls) == 1

        script = Script(
            error(503), ok(run_update("running"), COMPLETE), ok(run_update("succeeded"))
        )
        async with aclient(script) as hot:
            assert (await hot.runs.wait("r1", max_attempts=1))["status"] == "succeeded"

        script = Script(ok(PUBLISHED), error(503), *[ok(COMPLETE) for _ in range(4)], ok(stop()))
        async with aclient(script) as hot:
            events = [
                event
                async for event in hot.streams.subscribe_with_event(
                    {"event_type": "x", "event_data": {}}, max_attempts=1
                )
            ]
        assert events[-1]["type"] == "run:stop"

        script = Script(ok(PUBLISHED), ok(COMPLETE), error(404))
        async with aclient(script) as hot:
            with pytest.raises(HotApiError) as raised:
                await hot.events.call_hot("::f/g")
        assert raised.value.status_code == 404

        script = Script(error(503, retry_after="60"))
        async with aclient(script) as hot:
            with pytest.raises(TimeoutError):
                await hot.streams.wait_for_run_result("s1", "e1", timeout=2)

    asyncio.run(scenario())
    assert sleeps[0] > 0
