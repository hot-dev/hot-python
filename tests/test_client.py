from __future__ import annotations

import json

import httpx
import pytest

from hot import AsyncHotClient, HotApiError, HotClient


def make_client(handler):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HotClient(token="test-token", base_url="https://api.example.test", client=client)


def make_async_client(handler):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return AsyncHotClient(token="test-token", base_url="https://api.example.test", client=client)


def envelope(data):
    return {"data": data, "meta": {"request_id": "req_123", "timestamp": "now"}}


def list_envelope(data):
    return {
        "data": data,
        "meta": {"request_id": "req_123", "timestamp": "now"},
        "pagination": {"page": 1, "per_page": 20, "total": len(data), "total_pages": 1},
    }


def test_publish_event_sends_bearer_token_and_returns_data():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["auth"] = request.headers["authorization"]
        seen["user_agent"] = request.headers["user-agent"]
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=envelope({"event_id": "evt_123", "stream_id": "str_123"}))

    with make_client(handler) as hot:
        event = hot.events.publish(
            {
                "event_type": "team-agent:ask",
                "event_data": {"question": "what changed?"},
            }
        )

    assert event["event_id"] == "evt_123"
    assert seen.pop("user_agent").startswith("hot-sdk-python/")
    assert seen == {
        "method": "POST",
        "path": "/v1/events",
        "auth": "Bearer test-token",
        "body": {
            "event_type": "team-agent:ask",
            "event_data": {"question": "what changed?"},
        },
    }


def test_retries_429_with_retry_after():
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(
                429,
                json={"error": {"code": "rate_limited", "message": "Slow down"}},
                headers={"Retry-After": "1"},
            )
        return httpx.Response(200, json=envelope({"id": "run_123"}))

    with make_client(handler) as hot:
        run = hot.runs.get("run_123")

    assert run["id"] == "run_123"
    assert calls["count"] == 2


def test_stream_error_responses_raise_structured_api_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "No such stream"}},
        )

    with make_client(handler) as hot:
        with pytest.raises(HotApiError) as exc:
            list(hot.streams.subscribe("missing-stream"))

    assert exc.value.status_code == 404
    assert exc.value.code == "not_found"


def test_429_without_retry_after_is_not_retried():
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(
            429,
            json={"error": {"code": "rate_limited", "message": "Slow down"}},
        )

    with make_client(handler) as hot:
        with pytest.raises(HotApiError) as exc:
            hot.runs.get("run_123")

    assert exc.value.status_code == 429
    assert calls["count"] == 1


def test_list_methods_return_api_list_envelope():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/runs"
        assert request.url.params["status"] == "completed"
        return httpx.Response(200, json=list_envelope([{"id": "run_123"}]))

    with make_client(handler) as hot:
        response = hot.runs.list(status="completed", cursor=None)

    assert response["data"] == [{"id": "run_123"}]
    assert response["pagination"]["total"] == 1


def test_streams_parse_sse_events():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/streams/subscribe-with-event"
        assert json.loads(request.content)["event_type"] == "team-agent:ask"
        return httpx.Response(
            200,
            content=(
                'data: {"type":"stream:data","data_type":"text","payload":"hi"}\n\n'
                'data: {"type":"run:stop","run":{"result":"done"}}\n\n'
            ),
            headers={"content-type": "text/event-stream"},
        )

    with make_client(handler) as hot:
        events = list(hot.streams.subscribe_with_event({"event_type": "team-agent:ask"}))

    assert events == [
        {"type": "stream:data", "data_type": "text", "payload": "hi"},
        {"type": "run:stop", "run": {"result": "done"}},
    ]


def test_api_error_is_structured():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={
                "error": {
                    "code": "unauthorized",
                    "message": "Bad token",
                    "request_id": "req_123",
                    "retry_after": 5,
                }
            },
            headers={"Retry-After": "5"},
        )

    with make_client(handler) as hot:
        with pytest.raises(HotApiError) as exc:
            hot.events.list()

    assert exc.value.status_code == 401
    assert exc.value.code == "unauthorized"
    assert exc.value.request_id == "req_123"
    assert exc.value.retry_after == 5

def test_call_hot_uses_nested_run_failure_result():
    responses = iter(
        [
            httpx.Response(
                200,
                json=envelope(
                    {
                        "event_id": "evt_123",
                        "stream_id": "str_123",
                    }
                ),
            ),
            httpx.Response(
                200,
                content=(
                    'data: {"type":"run:start","run":{"run_id":"run_123",'
                    '"event_id":"evt_123"}}\n\n'
                    'data: {"type":"run:fail","run":{"run_id":"run_123",'
                    '"event_id":"evt_123","status":"failed",'
                    '"result":{"$err":{"message":"boom"}}}}\n\n'
                ),
                headers={"content-type": "text/event-stream"},
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return next(responses)

    with make_client(handler) as hot:
        with pytest.raises(RuntimeError, match="boom"):
            hot.events.call_hot("demo/fn")


def test_call_hot_surfaces_hot_run_failure_shape():
    responses = iter(
        [
            httpx.Response(
                200,
                json=envelope(
                    {
                        "event_id": "evt_123",
                        "stream_id": "str_123",
                    }
                ),
            ),
            httpx.Response(
                200,
                content=(
                    'data: {"type":"run:start","run":{"run_id":"run_123",'
                    '"event_id":"evt_123"}}\n\n'
                    'data: {"type":"run:fail","run":{"run_id":"run_123",'
                    '"event_id":"evt_123","status":"failed",'
                    '"result":{"$type":"::hot::run/Failure",'
                    '"$origin":{"function":"::hot::exec/fail"},'
                    '"$val":{"msg":"too hot","err":"too hot"}}}}\n\n'
                ),
                headers={"content-type": "text/event-stream"},
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return next(responses)

    with make_client(handler) as hot:
        with pytest.raises(RuntimeError, match="too hot"):
            hot.events.call_hot("demo/fn")


def test_async_client_exposes_core_resources():
    async def run():
        async with make_async_client(lambda request: httpx.Response(200, json=envelope({}))) as hot:
            assert hot.projects is not None
            assert hot.files is not None
            assert hot.env is not None
            assert hot.streams is not None

    import asyncio

    asyncio.run(run())
