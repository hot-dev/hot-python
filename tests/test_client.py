from __future__ import annotations

import json

import httpx
import pytest

from hot import HotApiError, HotClient


def make_client(handler):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HotClient(token="test-token", base_url="https://api.example.test", client=client)


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
    assert seen == {
        "method": "POST",
        "path": "/v1/events",
        "auth": "Bearer test-token",
        "body": {
            "event_type": "team-agent:ask",
            "event_data": {"question": "what changed?"},
        },
    }


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
                }
            },
        )

    with make_client(handler) as hot:
        with pytest.raises(HotApiError) as exc:
            hot.events.list()

    assert exc.value.status_code == 401
    assert exc.value.code == "unauthorized"
    assert exc.value.request_id == "req_123"
