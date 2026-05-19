from __future__ import annotations

import asyncio

import httpx
import pytest

from hot import AsyncHotClient, HotClient

_PUBLISHED = (
    'data: {"type":"event:published","stream_id":"s1",'
    '"event_id":"e1","event_type":"x"}'
)


def _sse(*lines: str) -> bytes:
    return ("\n\n".join(lines) + "\n\n").encode()


def make_handler(responses):
    calls: list[tuple[str, str]] = []
    iterator = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        try:
            body = next(iterator)
        except StopIteration as exc:
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            ) from exc
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "text/event-stream"},
        )

    return handler, calls


def make_sync_client(handler):
    transport = httpx.MockTransport(handler)
    return HotClient(
        token="test-token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=transport),
    )


def make_async_client(handler):
    transport = httpx.MockTransport(handler)
    return AsyncHotClient(
        token="test-token",
        base_url="https://api.example.test",
        client=httpx.AsyncClient(transport=transport),
    )


def test_subscribe_with_event_reconnects_via_stream_id():
    handler, calls = make_handler(
        [
            _sse(
                _PUBLISHED,
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
            ),
            _sse(
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
                'data: {"type":"run:stop","run":{"run_id":"r1","result":"done"}}',
            ),
        ]
    )

    with make_sync_client(handler) as hot:
        events = list(
            hot.streams.subscribe_with_event(
                {"event_type": "team-agent:ask", "event_data": {}}
            )
        )

    assert [e["type"] for e in events] == [
        "event:published",
        "run:start",
        "run:stop",
    ]
    assert calls == [
        ("POST", "/v1/streams/subscribe-with-event"),
        ("GET", "/v1/streams/s1/subscribe"),
    ]


def test_subscribe_with_event_reconnect_false_returns_after_first_connection():
    handler, calls = make_handler(
        [
            _sse(
                _PUBLISHED,
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
            ),
        ]
    )

    with make_sync_client(handler) as hot:
        events = list(
            hot.streams.subscribe_with_event(
                {"event_type": "x", "event_data": {}},
                reconnect=False,
            )
        )

    assert [e["type"] for e in events] == ["event:published", "run:start"]
    assert len(calls) == 1


def test_subscribe_with_event_dedupes_run_events_by_run_id():
    handler, _ = make_handler(
        [
            _sse(
                _PUBLISHED,
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
            ),
            _sse(
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
                'data: {"type":"run:start","run":{"run_id":"r2"}}',
                'data: {"type":"run:fail","run":{"run_id":"r1","result":"err"}}',
                'data: {"type":"run:fail","run":{"run_id":"r1","result":"err"}}',
            ),
        ]
    )

    with make_sync_client(handler) as hot:
        events = list(
            hot.streams.subscribe_with_event(
                {"event_type": "x", "event_data": {}}
            )
        )

    start_ids = [
        e["run"]["run_id"] for e in events if e["type"] == "run:start"
    ]
    fail_ids = [e["run"]["run_id"] for e in events if e["type"] == "run:fail"]

    assert start_ids == ["r1", "r2"]
    assert fail_ids == ["r1"]


def test_subscribe_with_event_raises_when_no_event_published():
    handler, _ = make_handler([b""])

    with make_sync_client(handler) as hot:
        with pytest.raises(RuntimeError, match="event:published"):
            list(
                hot.streams.subscribe_with_event(
                    {"event_type": "x", "event_data": {}}
                )
            )


def test_async_subscribe_with_event_reconnects():
    handler, calls = make_handler(
        [
            _sse(
                _PUBLISHED,
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
            ),
            _sse(
                'data: {"type":"run:start","run":{"run_id":"r1"}}',
                'data: {"type":"run:stop","run":{"run_id":"r1","result":"done"}}',
            ),
        ]
    )

    async def run():
        async with make_async_client(handler) as hot:
            events = []
            async for event in hot.streams.subscribe_with_event(
                {"event_type": "x", "event_data": {}}
            ):
                events.append(event)
            return events

    events = asyncio.run(run())
    assert [e["type"] for e in events] == [
        "event:published",
        "run:start",
        "run:stop",
    ]
    assert calls == [
        ("POST", "/v1/streams/subscribe-with-event"),
        ("GET", "/v1/streams/s1/subscribe"),
    ]
