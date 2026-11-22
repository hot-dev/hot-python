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
        events = list(
            hot.streams.subscribe_with_event({"event_type": "team-agent:ask"}, reconnect=False)
        )

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
                content=(
                    'data: {"type":"event:published","event_id":"evt_123",'
                    '"stream_id":"str_123"}\n\n'
                    'data: {"type":"run:start","run":{"run_id":"run_123",'
                    '"event_id":"evt_123"}}\n\n'
                    'data: {"type":"run:fail","run":{"run_id":"run_123",'
                    '"event_id":"evt_123","status":"failed",'
                    '"result":{"$err":{"message":"boom"}}}}\n\n'
                ),
                headers={"content-type": "text/event-stream"},
            )
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
                content=(
                    'data: {"type":"event:published","event_id":"evt_123",'
                    '"stream_id":"str_123"}\n\n'
                    'data: {"type":"run:start","run":{"run_id":"run_123",'
                    '"event_id":"evt_123"}}\n\n'
                    'data: {"type":"run:fail","run":{"run_id":"run_123",'
                    '"event_id":"evt_123","status":"failed",'
                    '"result":{"$type":"::hot::run/Failure",'
                    '"$origin":{"function":"::hot::exec/fail"},'
                    '"$val":{"msg":"too hot","err":"too hot"}}}}\n\n'
                ),
                headers={"content-type": "text/event-stream"},
            )
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


def test_status_hits_root_status_without_authorization_header():
    seen: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "method": request.method,
                "url": str(request.url),
                "has_auth": "authorization" in request.headers,
                "accept": request.headers.get("accept"),
                "user_agent": request.headers.get("user-agent"),
            }
        )
        return httpx.Response(200, json={"status": "ok", "version": "3.0.14"})

    with HotClient(
        token="secret-token",
        base_url="https://api.example.test/v1/",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        result = hot.status()

    assert result == {"status": "ok", "version": "3.0.14"}
    assert len(seen) == 1
    assert seen[0]["method"] == "GET"
    assert seen[0]["url"] == "https://api.example.test/status"
    assert seen[0]["has_auth"] is False
    assert seen[0]["accept"] == "application/json"
    assert str(seen[0]["user_agent"]).startswith("hot-sdk-python/")

    import asyncio

    async def scenario():
        async with AsyncHotClient(
            token="secret-token",
            base_url="https://api.example.test/v1///",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as ahot:
            return await ahot.status()

    assert asyncio.run(scenario()) == {"status": "ok", "version": "3.0.14"}
    assert len(seen) == 2
    assert seen[1]["url"] == "https://api.example.test/status"
    assert seen[1]["has_auth"] is False


def test_request_and_request_raw_normalize_leading_slash_sync_and_async():
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path.endswith("/raw"):
            return httpx.Response(200, content=b"raw-bytes")
        return httpx.Response(200, json=envelope({"ok": True}))

    with make_client(handler) as hot:
        assert hot.request("GET", "runs")["data"] == {"ok": True}
        assert hot.request("GET", "/runs")["data"] == {"ok": True}
        assert hot.request_raw("GET", "items/raw").content == b"raw-bytes"

    import asyncio

    async def scenario():
        async with make_async_client(handler) as ahot:
            assert (await ahot.request("GET", "runs"))["data"] == {"ok": True}
            assert (await ahot.request_raw("GET", "items/raw")).content == b"raw-bytes"

    asyncio.run(scenario())
    assert seen == [
        ("GET", "/v1/runs"),
        ("GET", "/v1/runs"),
        ("GET", "/v1/items/raw"),
        ("GET", "/v1/runs"),
        ("GET", "/v1/items/raw"),
    ]


def test_client_close_ownership_sync_and_async():
    external = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json={})))
    with HotClient(token="t", client=external):
        pass
    assert not external.is_closed
    external.close()

    owned = HotClient(token="t")
    with owned:
        assert not owned._transport.client.is_closed
    assert owned._transport.client.is_closed

    import asyncio

    async def scenario():
        ext_async = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))
        )
        async with AsyncHotClient(token="t", client=ext_async):
            pass
        assert not ext_async.is_closed
        await ext_async.aclose()

        owned_async = AsyncHotClient(token="t")
        async with owned_async:
            assert not owned_async._transport.client.is_closed
        assert owned_async._transport.client.is_closed

    asyncio.run(scenario())


def test_repr_redacts_token():
    secret = "hot_live_super_secret_token_value"
    with HotClient(token=secret, base_url="https://api.example.test") as hot:
        assert secret not in repr(hot)
        assert secret not in repr(hot._transport)
        assert "token='***'" in repr(hot)

    import asyncio

    async def scenario():
        async with AsyncHotClient(token=secret, base_url="https://api.example.test") as ahot:
            assert secret not in repr(ahot)
            assert secret not in repr(ahot._transport)
            assert "token='***'" in repr(ahot)

    asyncio.run(scenario())


def test_version_matches_pyproject_and_user_agent():
    import re
    from pathlib import Path

    import hot
    from hot._transport import USER_AGENT

    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject.read_text(), re.MULTILINE)
    assert match is not None
    expected = match.group(1)
    assert hot.__version__ == expected
    assert USER_AGENT == f"hot-sdk-python/{expected}"
