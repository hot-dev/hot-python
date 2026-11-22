from __future__ import annotations

import asyncio
from contextlib import aclosing

import httpx
import pytest

from hot import AsyncHotClient, HotClient
from hot.resources import enc, enc_path


def test_core_resource_methods_route_to_expected_api_paths():
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.method == "DELETE":
            return httpx.Response(204)
        if request.url.path.endswith("/download"):
            return httpx.Response(200, content=b"download")
        return httpx.Response(200, json={"data": {}, "meta": {}})

    with HotClient(
        token="test-token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        hot.projects.create({"name": "Demo"})
        hot.projects.update("demo", {"name": "Demo"})
        hot.projects.activate("demo")
        hot.projects.deactivate("demo")
        hot.projects.event_handlers("demo")
        hot.projects.schedules("demo")
        hot.builds.list_for_project("demo")
        hot.builds.get("demo", "build_123")
        hot.builds.deployed("demo")
        hot.builds.live("demo")
        hot.builds.download("demo", "build_123")
        hot.builds.deploy("demo", "build_123")
        hot.context.create("demo", {"key": "MODEL", "value": "gpt"})
        hot.context.update("demo", "MODEL", {"value": "claude"})
        hot.context.delete("demo", "MODEL")
        hot.domains.create({"domain": "example.com"})
        hot.domains.list()
        hot.domains.get("dom_123")
        hot.domains.verify("dom_123")
        hot.domains.delete("dom_123")
        hot.sessions.create({"permissions": {"event:*": ["create"]}})
        hot.sessions.revoke("ses_123")
        hot.sessions.revoke_all()
        hot.service_keys.create({"name": "CI", "permissions": {"event:*": ["create"]}})
        hot.service_keys.get("key_123")
        hot.service_keys.update("key_123", {"metadata": {"purpose": "deploy"}})
        hot.service_keys.revoke("key_123")
        hot.service_keys.revoke_all()
        hot.files.upload("notes/today.txt", b"hello")
        hot.files.initiate_upload({"path": "large.bin", "expected_size": 1024})
        hot.files.upload_part("upload_123", 1, b"part")
        hot.files.complete_upload("upload_123")
        hot.files.abort_upload("upload_123")
        hot.org.usage()

    assert seen == [
        ("POST", "/v1/projects"),
        ("PATCH", "/v1/projects/demo"),
        ("POST", "/v1/projects/demo/activate"),
        ("POST", "/v1/projects/demo/deactivate"),
        ("GET", "/v1/projects/demo/event-handlers"),
        ("GET", "/v1/projects/demo/schedules"),
        ("GET", "/v1/projects/demo/builds"),
        ("GET", "/v1/projects/demo/builds/build_123"),
        ("GET", "/v1/projects/demo/builds/deployed"),
        ("GET", "/v1/projects/demo/builds/live"),
        ("GET", "/v1/projects/demo/builds/build_123/download"),
        ("POST", "/v1/projects/demo/builds/build_123/deploy"),
        ("POST", "/v1/projects/demo/context"),
        ("PUT", "/v1/projects/demo/context/MODEL"),
        ("DELETE", "/v1/projects/demo/context/MODEL"),
        ("POST", "/v1/domains"),
        ("GET", "/v1/domains"),
        ("GET", "/v1/domains/dom_123"),
        ("POST", "/v1/domains/dom_123/verify"),
        ("DELETE", "/v1/domains/dom_123"),
        ("POST", "/v1/sessions"),
        ("DELETE", "/v1/sessions/ses_123"),
        ("DELETE", "/v1/sessions"),
        ("POST", "/v1/service-keys"),
        ("GET", "/v1/service-keys/key_123"),
        ("PATCH", "/v1/service-keys/key_123"),
        ("DELETE", "/v1/service-keys/key_123"),
        ("DELETE", "/v1/service-keys"),
        ("PUT", "/v1/files/upload/notes/today.txt"),
        ("POST", "/v1/files/uploads"),
        ("PUT", "/v1/files/uploads/upload_123/1"),
        ("POST", "/v1/files/uploads/upload_123/complete"),
        ("DELETE", "/v1/files/uploads/upload_123"),
        ("GET", "/v1/org/usage"),
    ]


def test_blobs_download_returns_the_raw_bytes():
    seen: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.url.raw_path.decode()))
        return httpx.Response(
            200,
            content=b"\x00\x01blob",
            headers={"Content-Type": "application/octet-stream"},
        )

    with HotClient(
        token="test-token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        assert hot.blobs.download("blob_123").content == b"\x00\x01blob"
        with hot.blobs.download_stream("blob/123") as response:
            assert b"".join(response.iter_bytes()) == b"\x00\x01blob"

    assert seen == [
        ("GET", "/v1/blobs/blob_123/download", "/v1/blobs/blob_123/download"),
        ("GET", "/v1/blobs/blob/123/download", "/v1/blobs/blob%2F123/download"),
    ]


def test_async_blobs_download_returns_the_raw_bytes():
    seen: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.url.raw_path.decode()))
        return httpx.Response(
            200,
            content=b"\x00\x01blob",
            headers={"Content-Type": "application/octet-stream"},
        )

    async def scenario() -> None:
        async with AsyncHotClient(
            token="test-token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as hot:
            assert (await hot.blobs.download("blob_123")).content == b"\x00\x01blob"
            async with hot.blobs.download_stream("blob/123") as response:
                assert b"".join([chunk async for chunk in response.aiter_bytes()]) == (
                    b"\x00\x01blob"
                )

    asyncio.run(scenario())

    assert seen == [
        ("GET", "/v1/blobs/blob_123/download", "/v1/blobs/blob_123/download"),
        ("GET", "/v1/blobs/blob/123/download", "/v1/blobs/blob%2F123/download"),
    ]


def test_path_segment_validation_rejects_traversal_and_empty_before_http():
    def forbid_http(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"Unexpected HTTP call: {request.method} {request.url}")

    with HotClient(
        token="test-token",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(forbid_http)),
    ) as hot:
        with pytest.raises(ValueError):
            hot.sessions.revoke(".")
        with pytest.raises(ValueError):
            hot.service_keys.revoke(".")
        with pytest.raises(ValueError):
            hot.context.delete("p1", "..")
        with pytest.raises(ValueError):
            hot.projects.get("")
        with pytest.raises(ValueError):
            hot.files.upload("../secret", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("/../secret", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("a/./b", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("a//b", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("a/b/", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("/", b"x")
        with pytest.raises(ValueError):
            hot.files.upload("", b"x")

    async def async_scenario() -> None:
        async with AsyncHotClient(
            token="test-token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(forbid_http)),
        ) as ahot:
            with pytest.raises(ValueError):
                await ahot.sessions.revoke(".")
            with pytest.raises(ValueError):
                await ahot.service_keys.revoke(".")
            with pytest.raises(ValueError):
                await ahot.context.delete("p1", "..")
            with pytest.raises(ValueError):
                await ahot.projects.get("")
            with pytest.raises(ValueError):
                await ahot.files.upload("../secret", b"x")

    asyncio.run(async_scenario())

    assert enc("ok/seg") == "ok%2Fseg"
    assert enc_path("/notes/today.txt") == "notes/today.txt"
    assert enc_path("notes/today.txt") == "notes/today.txt"


def test_async_core_resource_methods_route_to_expected_api_paths():
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.method == "DELETE" and not request.url.path.endswith(
            ("/sessions", "/service-keys", "/calls")
        ):
            return httpx.Response(204)
        if request.url.path.endswith("/download"):
            return httpx.Response(200, content=b"download")
        return httpx.Response(200, json={"data": {}, "meta": {}})

    async def scenario() -> None:
        async with AsyncHotClient(
            token="test-token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as hot:
            await hot.events.publish({"event_type": "x", "event_data": {}})
            await hot.events.list()
            await hot.events.get("e1")
            await hot.events.get_runs("e1")
            await hot.runs.list(status="succeeded", time_range="P7D")
            await hot.runs.stats()
            await hot.runs.get("r1")
            await hot.tasks.get("t1")
            await hot.env.get()
            await hot.org.usage()
            await hot.projects.create({"name": "Demo"})
            await hot.projects.get("demo")
            await hot.projects.update("demo", {"name": "Demo"})
            await hot.projects.delete("demo")
            await hot.projects.activate("demo")
            await hot.projects.deactivate("demo")
            await hot.projects.event_handlers("demo")
            await hot.projects.schedules("demo")
            await hot.builds.list()
            await hot.builds.list_for_project("demo")
            await hot.builds.get("demo", "b1")
            await hot.builds.deployed("demo")
            await hot.builds.live("demo")
            await hot.builds.download("demo", "b1")
            await hot.builds.deploy("demo", "b1")
            await hot.context.list("demo")
            await hot.context.create("demo", {"key": "K", "value": "V"})
            await hot.context.update("demo", "K", {"value": "V2"})
            await hot.context.delete("demo", "K")
            await hot.domains.create({"domain": "example.com"})
            await hot.domains.list()
            await hot.domains.get("d1")
            await hot.domains.verify("d1")
            await hot.domains.delete("d1")
            await hot.sessions.create({"permissions": {}})
            await hot.sessions.list()
            await hot.sessions.revoke("s1")
            await hot.sessions.revoke_all()
            await hot.service_keys.create({"name": "CI", "permissions": {}})
            await hot.service_keys.list()
            await hot.service_keys.get("sk1")
            await hot.service_keys.update("sk1", {"metadata": {}})
            await hot.service_keys.revoke("sk1")
            await hot.service_keys.revoke_all()
            await hot.files.list()
            await hot.files.get("f1")
            await hot.files.delete("f1")
            await hot.files.download("f1")
            await hot.files.upload("/notes/today.txt", b"hello")
            await hot.files.initiate_upload({"path": "big.bin"})
            await hot.files.upload_part("u1", 1, b"part")
            await hot.files.complete_upload("u1")
            await hot.files.abort_upload("u1")

    asyncio.run(scenario())
    assert ("PUT", "/v1/files/upload/notes/today.txt") in seen
    assert ("DELETE", "/v1/projects/demo") in seen
    assert ("DELETE", "/v1/files/f1") in seen
    assert ("DELETE", "/v1/files/uploads/u1") in seen


@pytest.mark.parametrize(
    "open_stream",
    [
        lambda hot: hot.runs.subscribe("r1"),
        lambda hot: hot.env.subscribe(),
        lambda hot: hot.streams.subscribe("s1"),
        lambda hot: hot.streams.subscribe_post("s1"),
        lambda hot: hot.streams.subscribe_with_event({"event_type": "x"}, reconnect=False),
        lambda hot: hot.tasks.subscribe("t1"),
    ],
    ids=[
        "runs.subscribe",
        "env.subscribe",
        "streams.subscribe",
        "streams.subscribe_post",
        "streams.subscribe_with_event(reconnect=False)",
        "tasks.subscribe",
    ],
)
def test_async_stream_wrappers_close_inner_response_immediately_on_early_exit(open_stream):
    class TrackingStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self):
            yield b'data: {"type":"stream:data","n":1}\n\n'
            yield b'data: {"type":"stream:data","n":2}\n\n'

        async def aclose(self) -> None:
            self.closed = True

    body = TrackingStream()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body, headers={"content-type": "text/event-stream"})

    async def scenario() -> None:
        async with AsyncHotClient(
            token="test-token",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as hot:
            async with aclosing(open_stream(hot)) as events:
                first = await events.__anext__()
                assert first == {"type": "stream:data", "n": 1}
                assert not body.closed
            # Must be closed immediately on exiting aclosing (not deferred to GC).
            assert body.closed

    asyncio.run(scenario())
