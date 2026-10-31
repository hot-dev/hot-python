from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from hot import AsyncHotClient, HotApiError, HotClient, HotKeyScopeError


def recorder():
    seen: list[tuple[str, str, str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, str(request.url.query, "ascii"), body))
        if request.method == "DELETE" and "/api-keys/" in request.url.path:
            return httpx.Response(204)
        return httpx.Response(200, json={"data": {"ok": True}, "meta": {}})

    return handler, seen


def sync_client(handler) -> HotClient:
    return HotClient(
        token="hot_org",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def async_client(handler) -> AsyncHotClient:
    return AsyncHotClient(
        token="hot_org",
        base_url="https://api.example.test",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


CHILD = {"name": "Globex", "slug": "globex", "idempotency_key": "leela-org-1"}
KEY = {"description": "events", "permissions": {"event:*": ["create"]}}

EXPECTED = [
    ("GET", "/v1/orgs", "limit=5", None),
    ("POST", "/v1/orgs", "", CHILD),
    ("GET", "/v1/orgs/o1", "", None),
    ("PATCH", "/v1/orgs/o1", "", {"state": "suspended"}),
    ("GET", "/v1/orgs/root/usage", "", None),
    ("DELETE", "/v1/orgs/o1/calls", "", None),
    ("DELETE", "/v1/orgs/o1/calls", "include_sub_orgs=true", None),
    ("GET", "/v1/orgs/o1/envs", "", None),
    ("POST", "/v1/orgs/o1/envs", "", {"name": "program"}),
    ("GET", "/v1/orgs/o1/envs/e1", "", None),
    ("GET", "/v1/orgs/root/api-keys", "", None),
    ("POST", "/v1/orgs/root/api-keys", "", KEY),
    ("GET", "/v1/orgs/o1/envs/e1/api-keys", "offset=20", None),
    ("POST", "/v1/orgs/o1/envs/e1/api-keys", "", KEY),
    ("GET", "/v1/api-keys/k1", "", None),
    ("PATCH", "/v1/api-keys/k1", "", {"permissions": {"event:*": ["create"]}}),
    ("POST", "/v1/api-keys/k1/rotate", "", {}),
    ("POST", "/v1/api-keys/k1/rotate", "", {"overlap_seconds": 0}),
    ("DELETE", "/v1/api-keys/k1", "", None),
    ("GET", "/v1/orgs/o1/envs/e1/projects", "", None),
    ("POST", "/v1/orgs/o1/envs/e1/projects", "", {"name": "p1"}),
    ("GET", "/v1/orgs/o1/envs/e1/projects/p1", "", None),
    ("GET", "/v1/orgs/o1/envs/e1/builds", "", None),
    ("POST", "/v1/orgs/o1/envs/e1/projects/p1/builds/b1/deploy", "", None),
    ("POST", "/v1/orgs/o1/envs/e1/projects/p1/context", "", {"key": "k", "value": "v"}),
    ("PUT", "/v1/orgs/o1/envs/e1/projects/p1/context/k", "", {"value": "w"}),
    ("DELETE", "/v1/env/calls", "", None),
    ("DELETE", "/v1/runs/r1/calls", "", None),
    ("DELETE", "/v1/streams/s1/calls", "", None),
]


def test_org_api_methods_route_to_org_paths():
    handler, seen = recorder()
    with sync_client(handler) as hot:
        hot.orgs.list(limit=5)
        hot.orgs.create(CHILD)
        hot.orgs.get("o1")
        hot.orgs.update("o1", {"state": "suspended"})
        hot.orgs.usage("root")
        hot.orgs.purge_calls("o1")
        hot.orgs.purge_calls("o1", include_sub_orgs=True)
        hot.orgs.list_envs("o1")
        hot.orgs.create_env("o1", {"name": "program"})
        hot.orgs.get_env("o1", "e1")
        hot.orgs.list_api_keys("root")
        hot.orgs.create_api_key("root", KEY)
        hot.orgs.list_env_api_keys("o1", "e1", offset=20)
        hot.orgs.create_env_api_key("o1", "e1", KEY)
        hot.api_keys.get("k1")
        hot.api_keys.update("k1", {"permissions": {"event:*": ["create"]}})
        hot.api_keys.rotate("k1")
        hot.api_keys.rotate("k1", overlap_seconds=0)
        assert hot.api_keys.revoke("k1") is None
        env = hot.orgs.env("o1", "e1")
        assert (env.org_id, env.env_id) == ("o1", "e1")
        env.projects.list()
        env.projects.create({"name": "p1"})
        env.projects.get("p1")
        env.builds.list()
        env.builds.deploy("p1", "b1")
        env.context.create("p1", {"key": "k", "value": "v"})
        env.context.update("p1", "k", {"value": "w"})
        hot.env.purge_calls()
        hot.runs.purge_calls("r1")
        hot.streams.purge_calls("s1")
    assert seen == EXPECTED


def test_async_org_api_methods_route_to_org_paths():
    handler, seen = recorder()

    async def run():
        async with async_client(handler) as hot:
            await hot.orgs.list(limit=5)
            await hot.orgs.create(CHILD)
            await hot.orgs.get("o1")
            await hot.orgs.update("o1", {"state": "suspended"})
            await hot.orgs.usage("root")
            await hot.orgs.purge_calls("o1")
            await hot.orgs.purge_calls("o1", include_sub_orgs=True)
            await hot.orgs.list_envs("o1")
            await hot.orgs.create_env("o1", {"name": "program"})
            await hot.orgs.get_env("o1", "e1")
            await hot.orgs.list_api_keys("root")
            await hot.orgs.create_api_key("root", KEY)
            await hot.orgs.list_env_api_keys("o1", "e1", offset=20)
            await hot.orgs.create_env_api_key("o1", "e1", KEY)
            await hot.api_keys.get("k1")
            await hot.api_keys.update("k1", {"permissions": {"event:*": ["create"]}})
            await hot.api_keys.rotate("k1")
            await hot.api_keys.rotate("k1", overlap_seconds=0)
            assert await hot.api_keys.revoke("k1") is None
            env = hot.orgs.env("o1", "e1")
            await env.projects.list()
            await env.projects.create({"name": "p1"})
            await env.projects.get("p1")
            await env.builds.list()
            await env.builds.deploy("p1", "b1")
            await env.context.create("p1", {"key": "k", "value": "v"})
            await env.context.update("p1", "k", {"value": "w"})
            await hot.env.purge_calls()
            await hot.runs.purge_calls("r1")
            await hot.streams.purge_calls("s1")

    asyncio.run(run())
    assert seen == EXPECTED


def test_org_env_segments_are_percent_encoded():
    raw: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        raw.append(request.url.raw_path)
        return httpx.Response(200, json={"data": {}, "meta": {}})

    with sync_client(handler) as hot:
        hot.orgs.env("o/1", "e 1").projects.get("p/1")
    assert raw == [b"/v1/orgs/o%2F1/envs/e%201/projects/p%2F1"]


def scope_mismatch(message: str) -> httpx.Response:
    return httpx.Response(
        403,
        json={"error": {"code": "key_scope_mismatch", "message": message, "request_id": "req_1"}},
    )


def test_org_key_on_runtime_route_raises_key_scope_error_for_env():
    with sync_client(lambda request: scope_mismatch("needs an environment API key")) as hot:
        with pytest.raises(HotKeyScopeError) as error:
            hot.env.get()
    assert error.value.required_scope == "env"
    assert error.value.status_code == 403
    assert error.value.code == "key_scope_mismatch"
    assert error.value.request_id == "req_1"
    assert "environment API key" in str(error.value)
    assert isinstance(error.value, HotApiError)


def test_env_key_on_org_route_raises_key_scope_error_for_org():
    with sync_client(lambda request: scope_mismatch("needs an org API key")) as hot:
        for call in (
            hot.orgs.list,
            lambda: hot.api_keys.get("k1"),
            lambda: hot.orgs.env("o1", "e1").projects.list(),
        ):
            with pytest.raises(HotKeyScopeError) as error:
                call()
            assert error.value.required_scope == "org"
            assert "org API key" in str(error.value)


def test_async_key_scope_error():
    async def run():
        async with async_client(lambda request: scope_mismatch("x")) as hot:
            with pytest.raises(HotKeyScopeError) as error:
                await hot.orgs.get("o1")
            assert error.value.required_scope == "org"

    asyncio.run(run())


def test_other_403_stays_plain_api_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": {"code": "forbidden", "message": "no"}})

    with sync_client(handler) as hot:
        with pytest.raises(HotApiError) as error:
            hot.orgs.get("o1")
    assert not isinstance(error.value, HotKeyScopeError)
