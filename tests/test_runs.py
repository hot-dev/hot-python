from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from hot import AsyncHotClient, HotClient, HotRunError


def run_response(status: str, result=None) -> httpx.Response:
    run = {
        "run_id": "r1",
        "env_id": "env1",
        "stream_id": "s1",
        "build_id": "b1",
        "run_type": "event",
        "status": status,
        "start_time": "2026-01-01T00:00:00Z",
        "stop_time": None if status == "running" else "2026-01-01T00:00:01Z",
        "origin_run_id": None,
        "event_id": "e1",
        "result": result,
        "project_id": "p1",
        "project_name": "fixture",
        "retry_attempt": 0,
    }
    event = {"type": "run:update", "run": run}
    return httpx.Response(
        200,
        content=f"data: {json.dumps(event)}\n\n",
        headers={"content-type": "text/event-stream"},
    )


def test_run_wait_reconnects_and_returns_succeeded_snapshot():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return run_response("running" if calls == 1 else "succeeded", {"answer": 42})

    with HotClient(
        token="token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        run = hot.runs.wait("r1")

    assert run["result"] == {"answer": 42}
    assert calls == 2


def test_run_wait_raises_structured_failure():
    with HotClient(
        token="token",
        base_url="https://api.example.test",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: run_response("failed", {"error": "boom"}))
        ),
    ) as hot:
        with pytest.raises(HotRunError, match="boom") as error:
            hot.runs.wait("r1")
    assert error.value.run["status"] == "failed"


def test_async_run_wait_returns_succeeded_snapshot():
    async def run():
        async with AsyncHotClient(
            token="token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: run_response("succeeded", "done"))
            ),
        ) as hot:
            return await hot.runs.wait("r1")

    assert asyncio.run(run())["result"] == "done"
