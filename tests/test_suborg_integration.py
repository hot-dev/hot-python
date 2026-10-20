"""Sub-org contract against a real Hot server, including PostgreSQL restricted roles.

Run integration/suborg_fixture.py; credentials always belong to a disposable DB.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path

import pytest

from hot import (
    AsyncHotClient,
    AsyncHotManagementClient,
    HotApiError,
    HotClient,
    HotManagementClient,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("HOT_TEST_SUBORG_FIXTURE"), reason="run integration/suborg_fixture.py"
)


def fixture():
    return json.loads(Path(os.environ["HOT_TEST_SUBORG_FIXTURE"]).read_text())


def runtime(token):
    return HotClient(token=token, base_url=os.environ["HOT_TEST_BASE_URL"], timeout=10)


def denied(call, statuses=(401, 403, 404)):
    with pytest.raises(HotApiError) as error:
        call()
    assert error.value.status_code in statuses


def test_suborg_provisioning_and_isolation():
    conf = fixture()
    with HotManagementClient(
        token=conf["management_token"], base_url=os.environ["HOT_TEST_BASE_URL"], timeout=10
    ) as management:
        children, clients, keys = [], [], []
        try:
            for _ in range(2):
                identity = uuid.uuid4().hex
                body = {
                    "name": "SDK Customer",
                    "slug": "sdk-" + identity,
                    "external_id": identity,
                    "idempotency_key": identity,
                }
                child = management.create_child(body)
                replay = management.create_child(body)
                assert replay["org"]["org_id"] == child["org"]["org_id"]
                assert replay["created"] is False
                child_id = child["org"]["org_id"]
                children.append(child_id)
                env = management.create_environment(child_id, {"name": "production"})
                assert management.create_environment(child_id, {"name": "production"}) == env
                key = management.create_api_key(
                    child_id, env["env_id"], {"description": "sdk", "permissions": {"*:*": ["*"]}}
                )
                keys.append(key)
                client = runtime(key["token"])
                clients.append(client)
                assert client.env.get()["org_id"] == child_id
                usage = client.org.usage()
                assert usage["env_id"] == env["env_id"]
                assert usage["usage_scope"] == "environment"
                assert usage["limits_scope"] == "billing_account"
                assert usage["usage"]["team_members"] == 0
            a, b = clients
            pa, pb = (
                a.projects.create({"name": "shared-name"}),
                b.projects.create({"name": "shared-name"}),
            )
            assert pa["project_id"] != pb["project_id"]
            assert a.projects.get("shared-name")["project_id"] == pa["project_id"]
            assert b.projects.get("shared-name")["project_id"] == pb["project_id"]
            denied(lambda: b.projects.get(pa["project_id"]), (404,))
            with runtime(conf["root_token"]) as root:
                denied(lambda: root.projects.get(pa["project_id"]), (404,))
            event = a.events.publish({"event_type": "sdk:isolation", "event_data": {"owner": "a"}})
            assert a.events.get(event["event_id"])["event_data"] == {"owner": "a"}
            denied(lambda: b.events.get(event["event_id"]), (404,))
            denied(lambda: next(b.streams.subscribe(event["stream_id"])))
            # A runtime key is not a management credential, nor vice versa.
            with HotManagementClient(
                token=keys[0]["token"], base_url=os.environ["HOT_TEST_BASE_URL"]
            ) as bad:
                denied(bad.list_children, (401,))
            with runtime(conf["management_token"]) as bad:
                denied(bad.env.get, (401,))
            assert management.list_children(limit=500)["total"] >= 2
            assert management.usage()["root_org_id"] == conf["root_org_id"]
            restricted = management.create_api_key(
                children[0],
                keys[0]["env_id"],
                {"description": "event-only", "permissions": {"event:sdk:*": ["create", "read"]}},
            )
            with runtime(restricted["token"]) as client:
                client.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                denied(client.projects.list, (403,))
            permissions = {"event:sdk:*": ["create", "read"], "stream:*": ["read"]}
            service = a.service_keys.create({"name": "sdk-service", "permissions": permissions})
            session = a.sessions.create({"permissions": permissions})
            for credential in (service, session):
                with runtime(credential["token"]) as client:
                    client.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                    denied(
                        lambda: client.events.get(
                            b.events.publish({"event_type": "sdk:isolation", "event_data": {}})[
                                "event_id"
                            ]
                        ),
                        (403, 404),
                    )
            a.service_keys.revoke(service["service_key_id"])
            a.sessions.revoke(session["session_id"])
            for credential in (service, session):
                with runtime(credential["token"]) as client:
                    denied(
                        lambda: client.events.publish(
                            {"event_type": "sdk:isolation", "event_data": {}}
                        ),
                        (401,),
                    )
            management.set_child_state(children[0], "suspended")
            denied(a.env.get, (401, 403))
            b.env.get()
            management.set_child_state(children[0], "active")
            a.env.get()
        finally:
            for client in clients:
                client.close()
            for child_id in children:
                management.set_child_state(child_id, "archived")


def test_issuer_revocation_and_narrowing_affect_existing_descendants():
    conf = fixture()
    for token in conf["revoked"]:
        with runtime(token) as client:
            denied(
                lambda: client.events.publish({"event_type": "sdk:isolation", "event_data": {}}),
                (401,),
            )
    for token in conf["narrowed"]:
        with runtime(token) as client:
            client.events.publish({"event_type": "sdk:isolation", "event_data": {}})
            denied(client.projects.list, (403,))
            denied(
                lambda: client.events.publish({"event_type": "other:blocked", "event_data": {}}),
                (403,),
            )


def test_async_management_and_runtime():
    conf = fixture()

    async def run():
        async with AsyncHotManagementClient(
            token=conf["management_token"], base_url=os.environ["HOT_TEST_BASE_URL"]
        ) as management:
            assert (await management.list_children())["total"] >= 2
            assert (await management.usage())["root_org_id"] == conf["root_org_id"]
        async with AsyncHotClient(
            token=conf["narrowed"][0], base_url=os.environ["HOT_TEST_BASE_URL"]
        ) as client:
            await client.events.publish({"event_type": "sdk:isolation", "event_data": {}})
            with pytest.raises(HotApiError) as error:
                await client.projects.list()
            assert error.value.status_code == 403

    asyncio.run(run())
