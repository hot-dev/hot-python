"""Correlating runs with a published event: chunks before run:start, and retries."""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

from hot import AsyncHotClient, HotClient, _streaming

PUBLISHED = {"type": "event:published", "stream_id": "s1", "event_id": "e1"}


def sse(*events: dict) -> bytes:
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events).encode()


def run(event_type: str, run_id: str, status: str, event_id: str = "e1", **fields) -> dict:
    return {
        "type": event_type,
        "run": {"run_id": run_id, "event_id": event_id, "status": status, **fields},
    }


def chunk(run_id: str, text: str) -> dict:
    return {
        "type": "stream:data",
        "run_id": run_id,
        "data_type": "chunk",
        "payload": {"text": text},
    }


def serve(*connections: bytes):
    remaining = list(connections)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=remaining.pop(0), headers={"content-type": "text/event-stream"}
        )

    return HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ), AsyncHotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


# Hot today pushes stream:data and run:stop before run:start (PR 4 changes
# the server); the documented order is run:start first. Both must work.
CHUNKS_FIRST = (
    PUBLISHED,
    chunk("r1", "a"),
    chunk("r1", "b"),
    chunk("r1", "c"),
    run("run:stop", "r1", "succeeded", result="done"),
    run("run:start", "r1", "succeeded", result="done"),
)
DOCUMENTED = (
    PUBLISHED,
    run("run:start", "r1", "running"),
    chunk("r1", "a"),
    chunk("r1", "b"),
    chunk("r1", "c"),
    run("run:stop", "r1", "succeeded", result="done"),
)


@pytest.mark.parametrize("order", [CHUNKS_FIRST, DOCUMENTED], ids=["chunks-first", "documented"])
def test_call_hot_delivers_chunks_in_either_order(order):
    hot, ahot = serve(sse(*order), sse(*order))
    chunks: list[str] = []
    with hot:
        assert hot.events.call_hot("::fixture::sdk/stream-chunks", on_chunk=chunks.append) == "done"
    assert chunks == ["a", "b", "c"]

    async def scenario():
        received: list[str] = []
        async with ahot:
            result = await ahot.events.call_hot("::f/g", on_chunk=received.append)
        return result, received

    assert asyncio.run(scenario()) == ("done", ["a", "b", "c"])


def test_wait_for_run_result_ignores_other_runs_chunks():
    hot, _ = serve(
        sse(
            chunk("other", "x"),
            run("run:start", "other", "running", event_id="e2"),
            chunk("other", "y"),
            chunk("r1", "a"),
            run("run:stop", "other", "succeeded", event_id="e2"),
            run("run:stop", "r1", "succeeded", result="done"),
        )
    )
    chunks: list[str] = []
    with hot:
        result = hot.streams.wait_for_run_result("s1", "e1", on_chunk=chunks.append)
    assert result["run_id"] == "r1"
    assert chunks == ["a"]


# A retrying handler: the first attempt fails with pending_retry, and the retry
# run has a new event_id and origin_run_id pointing at the failed run.
RETRY = (
    PUBLISHED,
    run("run:fail", "r1", "pending_retry", result={"$val": {"msg": "first attempt"}}),
    chunk("r2", "recovered"),
    run("run:stop", "r2", "succeeded", event_id="e9", origin_run_id="r1", result="ok"),
    run("run:start", "r2", "succeeded", event_id="e9", origin_run_id="r1"),
    run("run:start", "r1", "failed"),
)


def test_wait_for_run_result_follows_pending_retry():
    hot, ahot = serve(sse(*RETRY), sse(*RETRY))
    chunks: list[str] = []
    with hot:
        result = hot.streams.wait_for_run_result("s1", "e1", on_chunk=chunks.append)
    assert (result["run_id"], result["result"]) == ("r2", "ok")
    assert chunks == ["recovered"]

    async def scenario():
        async with ahot:
            return await ahot.streams.wait_for_run_result("s1", "e1")

    assert asyncio.run(scenario())["run_id"] == "r2"


def test_subscribe_with_event_continues_past_pending_retry():
    hot, ahot = serve(sse(*RETRY), sse(*RETRY))
    with hot:
        events = list(hot.streams.subscribe_with_event({"event_type": "sdk:flaky"}))
    assert [event["type"] for event in events] == [
        "event:published",
        "run:fail",
        "stream:data",
        "run:stop",
    ]

    async def scenario():
        async with ahot:
            return [event async for event in ahot.streams.subscribe_with_event({"event_type": "x"})]

    assert asyncio.run(scenario())[-1]["run"]["run_id"] == "r2"


def test_exhausted_retries_fail():
    hot, _ = serve(
        sse(
            PUBLISHED,
            run("run:fail", "r1", "pending_retry"),
            run("run:fail", "r2", "failed", event_id="e9", origin_run_id="r1", result="gave up"),
        )
    )
    with hot:
        with pytest.raises(RuntimeError, match="gave up"):
            hot.events.call_hot("::f/g")


def test_cancelled_run_raises():
    hot, _ = serve(sse(PUBLISHED, run("run:cancel", "r1", "cancelled")))
    with hot:
        with pytest.raises(RuntimeError, match="Run cancelled"):
            hot.events.call_hot("::f/g")


# hot replays a retried first attempt lossily (hot 3.0.0 and d5dde71): before
# the retry is queued as run:stop with status pending_retry, and after it is
# queued (before the retry run exists) as run:fail with status failed and a
# raised retry_attempt.
RETRY_RUN = run("run:stop", "r2", "succeeded", event_id="e9", origin_run_id="r1", result="ok")


def held_failure(**fields) -> dict:
    return run("run:fail", "r1", "failed", retry_attempt=1, result="first attempt", **fields)


@pytest.mark.parametrize(
    "replay",
    [
        run("run:fail", "r1", "pending_retry", retry_attempt=1),
        run("run:stop", "r1", "pending_retry", retry_attempt=1),
        held_failure(),
    ],
    ids=["fail-pending-retry", "stop-pending-retry", "fail-failed-retried"],
)
def test_lossy_retry_replays_follow_the_retry_run(replay):
    hot, ahot = serve(sse(PUBLISHED, replay, RETRY_RUN), sse(PUBLISHED, replay, RETRY_RUN))
    with hot:
        assert hot.events.call_hot("::f/g") == "ok"

    async def scenario():
        async with ahot:
            return await ahot.streams.wait_for_run_result("s1", "e1")

    assert asyncio.run(scenario())["run_id"] == "r2"


def test_retry_run_seen_before_its_origin():
    hot, _ = serve(sse(PUBLISHED, RETRY_RUN, held_failure()))
    with hot:
        assert hot.streams.wait_for_run_result("s1", "e1")["run_id"] == "r2"


def test_retried_run_failure_after_its_retry_appeared_is_not_final():
    start = run("run:start", "r2", "running", event_id="e9", origin_run_id="r1")
    hot, _ = serve(
        sse(PUBLISHED, run("run:fail", "r1", "pending_retry"), start, held_failure(), RETRY_RUN)
    )
    with hot:
        assert hot.events.call_hot("::f/g") == "ok"


def slow_stream(*events: dict, then: float):
    """A stream that sends ``events``, then keepalives every ``then`` seconds."""

    def body():
        yield sse(*events)
        while True:
            time.sleep(then)
            yield b": keepalive\n\n"

    return body()


def test_held_failure_stands_when_no_retry_appears(monkeypatch):
    monkeypatch.setattr(_streaming, "RETRY_GRACE", 0.2)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=slow_stream(PUBLISHED, held_failure(), then=0.3),
            headers={"content-type": "text/event-stream"},
        )

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(RuntimeError, match="first attempt"):
            hot.events.call_hot("::f/g", timeout=30)


def test_failures_without_a_scheduled_retry_are_reported_at_once():
    # No retries configured: retry_attempt stays 0.
    hot, _ = serve(sse(PUBLISHED, run("run:fail", "r1", "failed", retry_attempt=0, result="no")))
    started = time.monotonic()
    with hot:
        with pytest.raises(RuntimeError, match="no"):
            hot.events.call_hot("::f/g")
    assert time.monotonic() - started < 1

    # The last retry fails: its attempt number is its origin's.
    hot, _ = serve(
        sse(
            PUBLISHED,
            run("run:fail", "r1", "pending_retry", retry_attempt=1),
            run(
                "run:fail",
                "r2",
                "failed",
                event_id="e9",
                origin_run_id="r1",
                retry_attempt=1,
                result="last",
            ),
        )
    )
    started = time.monotonic()
    with hot:
        with pytest.raises(RuntimeError, match="last"):
            hot.streams.wait_for_run_result("s1", "e1")
    assert time.monotonic() - started < 1
