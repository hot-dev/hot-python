from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from hot import AsyncHotClient, HotClient, HotTaskError


def task_response(status: str, result=None) -> httpx.Response:
    task = {
        "task_id": "t1",
        "env_id": "env1",
        "stream_id": "s1",
        "build_id": "b1",
        "function_name": "work",
        "task_type": "code",
        "status": status,
        "result": result,
        "timeout_ms": 60_000,
        "retry_attempt": 0,
        "created_at": "2026-01-01T00:00:00Z",
    }
    event = {"type": "task:update", "task": task}
    return httpx.Response(
        200,
        content=f"data: {json.dumps(event)}\n\n",
        headers={"content-type": "text/event-stream"},
    )


def test_task_wait_reconnects_and_returns_completed_snapshot():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return task_response("running" if calls == 1 else "completed", {"answer": 42})

    with HotClient(
        token="token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        task = hot.tasks.wait("t1")

    assert task["result"] == {"answer": 42}
    assert calls == 2


def test_task_wait_raises_structured_failure():
    with HotClient(
        token="token",
        base_url="https://api.example.test",
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: task_response("failed", {"error": "boom"})
            )
        ),
    ) as hot:
        with pytest.raises(HotTaskError, match="boom") as error:
            hot.tasks.wait("t1")
    assert error.value.task["status"] == "failed"


def test_async_task_wait_returns_completed_snapshot():
    async def run():
        async with AsyncHotClient(
            token="token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: task_response("completed", "done"))
            ),
        ) as hot:
            return await hot.tasks.wait("t1")

    assert asyncio.run(run())["result"] == "done"
