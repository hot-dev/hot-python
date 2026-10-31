from __future__ import annotations

import asyncio

import httpx
import pytest

from hot import AsyncHotClient, HotApiError, HotClient
from hot._transport import DEFAULT_TIMEOUT


def counting_body(chunks: int, consumed: list[int]):
    def body():
        for index in range(chunks):
            consumed.append(index)
            yield b"x" * 1024

    return body()


def test_download_stream_does_not_buffer_the_body():
    consumed: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path in (
            "/v1/files/f1/download",
            "/v1/projects/p1/builds/b1/download",
        )
        return httpx.Response(200, content=counting_body(64, consumed))

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with hot.files.download_stream("f1") as response:
            chunks = response.iter_raw()
            assert len(next(chunks)) == 1024
            assert len(consumed) < 4
            assert sum(len(chunk) for chunk in chunks) == 63 * 1024
        consumed.clear()
        with hot.builds.download_stream("p1", "b1") as response:
            next(response.iter_raw())
            assert len(consumed) < 4


def test_async_download_stream_does_not_buffer_the_body():
    consumed: list[int] = []

    async def body():
        for index in range(64):
            consumed.append(index)
            yield b"x" * 1024

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body())

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as hot:
            async with hot.files.download_stream("f1") as response:
                async for _chunk in response.aiter_raw():
                    break
                assert len(consumed) < 4
            consumed.clear()
            async with hot.orgs.env("o1", "e1").builds.download_stream("p1", "b1") as response:
                assert response.request.url.path == (
                    "/v1/orgs/o1/envs/e1/projects/p1/builds/b1/download"
                )

    asyncio.run(scenario())


def test_download_stream_raises_api_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "gone"}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError) as error:
            with hot.files.download_stream("f1"):
                pass
    assert error.value.status_code == 404


def test_default_timeout_is_finite():
    with HotClient(token="t") as hot:
        assert hot._transport.client.timeout == DEFAULT_TIMEOUT
    with HotClient(token="t", timeout=httpx.Timeout(None)) as hot:
        assert hot._transport.client.timeout == httpx.Timeout(None)


def test_idempotent_requests_retry_502_503_504(monkeypatch):
    monkeypatch.setattr("hot._transport.time.sleep", lambda _seconds: None)
    statuses = [503, 502, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        if status != 200:
            return httpx.Response(status, json={"error": {"code": "unavailable", "message": "x"}})
        return httpx.Response(200, json={"data": {"run_id": "r1"}, "meta": {}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        assert hot.runs.get("r1") == {"run_id": "r1"}


def test_non_idempotent_requests_do_not_retry_5xx(monkeypatch):
    monkeypatch.setattr("hot._transport.time.sleep", lambda _seconds: None)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(503, json={"error": {"code": "unavailable", "message": "x"}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError):
            hot.events.publish({"event_type": "x", "event_data": {}})
    assert calls == ["POST"]
