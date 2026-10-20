from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hot import HotClient, HotManagementClient

OPERATIONS_PATH = Path(__file__).with_name("openapi_operations.json")

COVERAGE: dict[str, dict[str, str | bool]] = {
    "GET /v1/management/children": {"sdk": "management.list_children"},
    "POST /v1/management/children": {"sdk": "management.create_child"},
    "PATCH /v1/management/children/{child_id}/state": {"sdk": "management.set_child_state"},
    "POST /v1/management/children/{child_id}/environments": {
        "sdk": "management.create_environment"
    },
    "POST /v1/management/children/{child_id}/environments/{env_id}/api-keys": {
        "sdk": "management.create_api_key"
    },
    "GET /v1/management/usage": {"sdk": "management.usage"},
    "GET /status": {
        "ignored": True,
        "reason": "Public health endpoint, not part of authenticated SDK resources.",
    },
    "GET /v1/projects": {"sdk": "projects.list"},
    "POST /v1/projects": {"sdk": "projects.create"},
    "GET /v1/projects/{project_id_or_slug}": {"sdk": "projects.get"},
    "PATCH /v1/projects/{project_id_or_slug}": {"sdk": "projects.update"},
    "DELETE /v1/projects/{project_id_or_slug}": {"sdk": "projects.delete"},
    "POST /v1/projects/{project_id_or_slug}/activate": {"sdk": "projects.activate"},
    "POST /v1/projects/{project_id_or_slug}/deactivate": {"sdk": "projects.deactivate"},
    "GET /v1/builds": {"sdk": "builds.list"},
    "GET /v1/projects/{project_id_or_slug}/builds": {"sdk": "builds.list_for_project"},
    "POST /v1/projects/{project_id_or_slug}/builds": {"sdk": "builds.upload"},
    "GET /v1/projects/{project_id_or_slug}/builds/deployed": {"sdk": "builds.deployed"},
    "GET /v1/projects/{project_id_or_slug}/builds/live": {"sdk": "builds.live"},
    "GET /v1/projects/{project_id_or_slug}/builds/{build_id}": {"sdk": "builds.get"},
    "GET /v1/projects/{project_id_or_slug}/builds/{build_id}/download": {
        "sdk": "builds.download",
    },
    "POST /v1/projects/{project_id_or_slug}/builds/{build_id}/deploy": {
        "sdk": "builds.deploy",
    },
    "GET /v1/projects/{project_id_or_slug}/context": {"sdk": "context.list"},
    "POST /v1/projects/{project_id_or_slug}/context": {"sdk": "context.create"},
    "PUT /v1/projects/{project_id_or_slug}/context/{key}": {"sdk": "context.update"},
    "DELETE /v1/projects/{project_id_or_slug}/context/{key}": {"sdk": "context.delete"},
    "GET /v1/runs": {"sdk": "runs.list"},
    "GET /v1/runs/stats": {"sdk": "runs.stats"},
    "GET /v1/runs/{run_id}": {"sdk": "runs.get"},
    "GET /v1/runs/{run_id}/subscribe": {"sdk": "runs.subscribe"},
    "GET /v1/tasks/{task_id}": {"sdk": "tasks.get"},
    "GET /v1/tasks/{task_id}/subscribe": {"sdk": "tasks.subscribe"},
    "POST /v1/events": {"sdk": "events.publish"},
    "GET /v1/events": {"sdk": "events.list"},
    "GET /v1/events/{event_id}": {"sdk": "events.get"},
    "GET /v1/events/{event_id}/runs": {"sdk": "events.get_runs"},
    "GET /v1/projects/{project_id_or_slug}/event-handlers": {
        "sdk": "projects.event_handlers",
    },
    "GET /v1/projects/{project_id_or_slug}/schedules": {"sdk": "projects.schedules"},
    "POST /v1/sessions": {"sdk": "sessions.create"},
    "GET /v1/sessions": {"sdk": "sessions.list"},
    "DELETE /v1/sessions": {"sdk": "sessions.revoke_all"},
    "DELETE /v1/sessions/{session_id}": {"sdk": "sessions.revoke"},
    "POST /v1/service-keys": {"sdk": "service_keys.create"},
    "GET /v1/service-keys": {"sdk": "service_keys.list"},
    "DELETE /v1/service-keys": {"sdk": "service_keys.revoke_all"},
    "GET /v1/service-keys/{service_key_id}": {"sdk": "service_keys.get"},
    "PATCH /v1/service-keys/{service_key_id}": {"sdk": "service_keys.update"},
    "DELETE /v1/service-keys/{service_key_id}": {"sdk": "service_keys.revoke"},
    "POST /v1/domains": {"sdk": "domains.create"},
    "GET /v1/domains": {"sdk": "domains.list"},
    "GET /v1/domains/{domain_id}": {"sdk": "domains.get"},
    "DELETE /v1/domains/{domain_id}": {"sdk": "domains.delete"},
    "POST /v1/domains/{domain_id}/verify": {"sdk": "domains.verify"},
    "GET /v1/env": {"sdk": "env.get"},
    "GET /v1/env/subscribe": {"sdk": "env.subscribe"},
    "GET /v1/org/usage": {"sdk": "org.usage"},
    "GET /v1/streams/{stream_id}/subscribe": {"sdk": "streams.subscribe"},
    "POST /v1/streams/{stream_id}/subscribe": {"sdk": "streams.subscribe_post"},
    "POST /v1/streams/subscribe-with-event": {"sdk": "streams.subscribe_with_event"},
    "GET /v1/files": {"sdk": "files.list"},
    "GET /v1/files/{file_id}": {"sdk": "files.get"},
    "DELETE /v1/files/{file_id}": {"sdk": "files.delete"},
    "GET /v1/files/{file_id}/download": {"sdk": "files.download"},
    "PUT /v1/files/upload/{path}": {"sdk": "files.upload"},
    "POST /v1/files/uploads": {"sdk": "files.initiate_upload"},
    "PUT /v1/files/uploads/{upload_id}/{part_number}": {"sdk": "files.upload_part"},
    "POST /v1/files/uploads/{upload_id}/complete": {"sdk": "files.complete_upload"},
    "DELETE /v1/files/uploads/{upload_id}": {"sdk": "files.abort_upload"},
}


def test_openapi_operations_have_sdk_coverage_or_explicit_ignore():
    operations = json.loads(OPERATIONS_PATH.read_text())
    operation_keys = {operation_key(operation) for operation in operations}

    assert set(COVERAGE) == operation_keys


def test_covered_operations_point_to_existing_sdk_methods():
    with HotClient(token="test-token") as hot:
        for key, entry in COVERAGE.items():
            if entry.get("ignored") is True:
                assert entry.get("reason"), key
                continue

            sdk = entry["sdk"]
            assert isinstance(sdk, str)
            assert callable(resolve_sdk_method(hot, sdk)), f"{key} -> hot.{sdk}"


def operation_key(operation: dict[str, Any]) -> str:
    return f"{operation['method'].upper()} {operation['path']}"


def resolve_sdk_method(client: HotClient, path: str) -> Any:
    value: Any = client
    if path.startswith("management."):
        with HotManagementClient(token="hm_test") as management:
            return getattr(management, path.split(".", 1)[1])
    for part in path.split("."):
        value = getattr(value, part)
    return value
