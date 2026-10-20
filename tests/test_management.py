from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from hot import (
    AsyncHotClient,
    AsyncHotManagementClient,
    HotApiError,
    HotClient,
    HotManagementClient,
)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_management_wire_contract(asynchronous):
    seen = []
    wire = []

    def handler(request):
        assert request.headers["authorization"] == "Bearer hm_test"
        wire.append(request.url.raw_path.split(b"?")[0])
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, body, dict(request.url.params)))
        if request.method == "PATCH":
            return httpx.Response(204)
        return httpx.Response(200, json={"data": {"marker": "response"}, "meta": {}})

    body = {"name": "Customer", "slug": "customer", "idempotency_key": "stable"}
    key = {"description": "sdk", "permissions": {"event:sdk:*": ["create"]}}

    async def run():
        async with AsyncHotManagementClient(
            token="hm_test", client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        ) as client:
            assert await client.list_children(limit=7, offset=3) == {"marker": "response"}
            assert await client.create_child(body) == {"marker": "response"}
            assert await client.set_child_state("child/one", "suspended") is None
            assert await client.create_environment("child/one", {"name": "prod"})
            assert await client.create_api_key("child/one", "env/two", key)
            assert await client.usage()

    if asynchronous:
        asyncio.run(run())
    else:
        with HotManagementClient(
            token="hm_test", client=httpx.Client(transport=httpx.MockTransport(handler))
        ) as client:
            assert client.list_children(limit=7, offset=3) == {"marker": "response"}
            assert client.create_child(body) == {"marker": "response"}
            assert client.set_child_state("child/one", "suspended") is None
            assert client.create_environment("child/one", {"name": "prod"})
            assert client.create_api_key("child/one", "env/two", key)
            assert client.usage()
    assert seen == [
        ("GET", "/v1/management/children", None, {"limit": "7", "offset": "3"}),
        ("POST", "/v1/management/children", body, {}),
        ("PATCH", "/v1/management/children/child/one/state", {"state": "suspended"}, {}),
        ("POST", "/v1/management/children/child/one/environments", {"name": "prod"}, {}),
        ("POST", "/v1/management/children/child/one/environments/env/two/api-keys", key, {}),
        ("GET", "/v1/management/usage", None, {}),
    ]
    assert wire[2] == b"/v1/management/children/child%2Fone/state"
    assert wire[4] == b"/v1/management/children/child%2Fone/environments/env%2Ftwo/api-keys"


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_stream_reconnect_stops_on_authorization_failure(status, asynchronous, monkeypatch):
    calls = []
    monkeypatch.setattr("hot.resources.time.sleep", lambda _: None)

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 1:
            return httpx.Response(
                200,
                text='data: {"type":"event:published","stream_id":"s","event_id":"e"}\n\n',
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(status, json={"error": {"code": "denied", "message": "revoked"}})

    async def run():
        async with AsyncHotClient(
            token="test", client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        ) as hot:
            with pytest.raises(HotApiError) as error:
                async for _ in hot.streams.subscribe_with_event(
                    {"event_type": "sdk:echo", "event_data": {}}
                ):
                    pass
            assert error.value.status_code == status

    if asynchronous:
        asyncio.run(run())
    else:
        with HotClient(
            token="test", client=httpx.Client(transport=httpx.MockTransport(handler))
        ) as hot:
            with pytest.raises(HotApiError) as error:
                list(hot.streams.subscribe_with_event({"event_type": "sdk:echo", "event_data": {}}))
        assert error.value.status_code == status
    assert len(calls) == 2
