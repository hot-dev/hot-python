from __future__ import annotations

import httpx

from hot import HotClient


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
        hot.projects.update("demo", {"display_name": "Demo"})
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
        hot.domains.create({"domain": "example.com", "project": "demo"})
        hot.domains.list()
        hot.domains.get("dom_123")
        hot.domains.verify("dom_123")
        hot.domains.delete("dom_123")
        hot.sessions.create({"name": "Web"})
        hot.sessions.revoke("ses_123")
        hot.sessions.revoke_all()
        hot.service_keys.create({"name": "CI"})
        hot.service_keys.get("key_123")
        hot.service_keys.update("key_123", {"name": "Deploy"})
        hot.service_keys.revoke("key_123")
        hot.service_keys.revoke_all()
        hot.files.upload("notes/today.txt", b"hello")
        hot.files.initiate_upload({"path": "large.bin", "size": 1024})
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
