"""Org API and sub-org isolation against a real Hot server, including PostgreSQL restricted roles.

Run integration/suborg_fixture.py; credentials always belong to a disposable DB.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest

from hot import AsyncHotClient, HotApiError, HotClient, HotKeyScopeError

pytestmark = pytest.mark.skipif(
    not os.environ.get("HOT_TEST_SUBORG_FIXTURE"), reason="run integration/suborg_fixture.py"
)


def fixture():
    return json.loads(os.environ["HOT_TEST_SUBORG_FIXTURE"])


def client(token):
    return HotClient(token=token, base_url=os.environ["HOT_TEST_BASE_URL"], timeout=10)


def denied(call, statuses=(401, 403, 404)):
    with pytest.raises(HotApiError) as error:
        call()
    assert error.value.status_code in statuses
    return error.value


def child_body():
    identity = uuid.uuid4().hex[:24]
    return {
        "name": "SDK Customer",
        "slug": "sdk-" + identity,
        "external_id": identity,
        "idempotency_key": identity,
    }


def test_org_api_provisioning_and_isolation():
    conf = fixture()
    with client(conf["org_token"]) as org:
        children, clients, keys = [], [], []
        try:
            for _ in range(2):
                body = child_body()
                child = org.orgs.create(body)
                replay = org.orgs.create(body)
                assert child["created"] is True and replay["created"] is False
                assert replay["org"]["org_id"] == child["org"]["org_id"]
                child_id = child["org"]["org_id"]
                children.append(child_id)
                assert org.orgs.get(child_id)["parent_org_id"] == conf["root_org_id"]
                env = org.orgs.create_env(child_id, {"name": "production"})
                assert org.orgs.create_env(child_id, {"name": "production"}) == env
                assert org.orgs.get_env(child_id, env["env_id"]) == env
                names = {item["name"] for item in org.orgs.list_envs(child_id)["data"]}
                assert "production" in names
                key = org.orgs.create_env_api_key(
                    child_id,
                    env["env_id"],
                    {"description": "sdk", "permissions": {"*:*": ["*"]}},
                )
                assert key["scope"] == "env" and key["token"]
                keys.append(key)
                runtime = client(key["token"])
                clients.append(runtime)
                assert runtime.env.get()["org_id"] == child_id
                usage = runtime.org.usage()
                assert usage["env_id"] == env["env_id"]
                assert usage["usage_scope"] == "environment"
            listed = {item["org_id"] for item in org.orgs.list(limit=500)["data"]}
            assert set(children) <= listed
            usage = org.orgs.usage(conf["root_org_id"])
            assert usage["org_id"] == conf["root_org_id"]
            assert usage["usage_scope"] == "account"

            a, b = clients
            pa = a.projects.create({"name": "shared-name"})
            pb = b.projects.create({"name": "shared-name"})
            assert pa["project_id"] != pb["project_id"]
            assert a.projects.get("shared-name")["project_id"] == pa["project_id"]
            denied(lambda: b.projects.get(pa["project_id"]), (404,))
            with client(conf["root_token"]) as root:
                denied(lambda: root.projects.get(pa["project_id"]), (404,))
            event = a.events.publish({"event_type": "sdk:isolation", "event_data": {"owner": "a"}})
            assert a.events.get(event["event_id"])["event_data"] == {"owner": "a"}
            denied(lambda: b.events.get(event["event_id"]), (404,))
            denied(lambda: next(b.streams.subscribe(event["stream_id"])))

            # Projects and context provisioned through the org key land in the
            # child's environment.
            scoped = org.orgs.env(children[0], keys[0]["env_id"])
            scoped.projects.create({"name": "provisioned"})
            scoped.context.create(
                "provisioned", {"key": "callback.url", "value": "https://example.test/hot"}
            )
            assert a.projects.get("provisioned")["name"] == "provisioned"
            assert [item["key"] for item in a.context.list("provisioned")["data"]] == [
                "callback.url"
            ]
            assert any(item["name"] == "provisioned" for item in scoped.projects.list()["data"])
            denied(lambda: b.projects.get("provisioned"), (404,))

            # Org keys and environment keys are not interchangeable.
            with client(conf["org_token"]) as wrong:
                error = denied(wrong.env.get, (403,))
                assert isinstance(error, HotKeyScopeError) and error.required_scope == "env"
            error = denied(a.orgs.list, (403,))
            assert isinstance(error, HotKeyScopeError) and error.required_scope == "org"

            restricted = org.orgs.create_env_api_key(
                children[0],
                keys[0]["env_id"],
                {"description": "event-only", "permissions": {"event:sdk:*": ["create", "read"]}},
            )
            with client(restricted["token"]) as limited:
                limited.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                denied(limited.projects.list, (403,))

            permissions = {"event:sdk:*": ["create", "read"], "stream:*": ["read"]}
            service = a.service_keys.create({"name": "sdk-service", "permissions": permissions})
            session = a.sessions.create({"permissions": permissions})
            for credential in (service, session):
                with client(credential["token"]) as scoped_client:
                    scoped_client.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                    foreign = b.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                    denied(
                        lambda c=scoped_client, f=foreign: c.events.get(f["event_id"]), (403, 404)
                    )
            a.service_keys.revoke(service["service_key_id"])
            a.sessions.revoke(session["session_id"])
            for credential in (service, session):
                with client(credential["token"]) as revoked:
                    denied(
                        lambda c=revoked: c.events.publish(
                            {"event_type": "sdk:isolation", "event_data": {}}
                        ),
                        (401,),
                    )

            suspended = org.orgs.update(children[0], {"state": "suspended"})
            assert suspended["lifecycle_state"] == "suspended"
            denied(a.env.get, (401, 403))
            b.env.get()
            assert org.orgs.update(children[0], {"state": "active"})["lifecycle_state"] == "active"
            a.env.get()
        finally:
            for runtime in clients:
                runtime.close()
            for child_id in children:
                org.orgs.update(child_id, {"state": "archived"})


def test_api_keys_rotate_narrow_and_revoke():
    conf = fixture()
    with client(conf["org_token"]) as org:
        child = org.orgs.create(child_body())
        child_id, env_id = child["org"]["org_id"], child["env_id"]
        try:
            key = org.orgs.create_env_api_key(
                child_id, env_id, {"description": "rotating", "permissions": {"*:*": ["*"]}}
            )
            assert org.api_keys.get(key["api_key_id"])["scope"] == "env"
            assert "token" not in org.api_keys.get(key["api_key_id"])
            listed = org.orgs.list_env_api_keys(child_id, env_id)["data"]
            assert key["api_key_id"] in {item["api_key_id"] for item in listed}

            rotated = org.api_keys.rotate(key["api_key_id"], overlap_seconds=0)
            assert rotated["token"] != key["token"]
            with client(rotated["token"]) as fresh:
                fresh.env.get()
            with client(key["token"]) as stale:
                denied(stale.env.get, (401,))

            narrowed = org.api_keys.update(
                key["api_key_id"], {"permissions": {"event:sdk:*": ["create"]}}
            )
            assert narrowed["permissions"] == {"event:sdk:*": ["create"]}
            with client(rotated["token"]) as fresh:
                fresh.events.publish({"event_type": "sdk:isolation", "event_data": {}})
                denied(fresh.projects.list, (403,))

            assert org.api_keys.revoke(key["api_key_id"]) is None
            with client(rotated["token"]) as fresh:
                denied(fresh.env.get, (401,))

            org_keys = org.orgs.list_api_keys(conf["root_org_id"])["data"]
            assert all(item["scope"] == "org" for item in org_keys)
        finally:
            org.orgs.update(child_id, {"state": "archived"})


def test_issuer_revocation_and_narrowing_affect_existing_descendants():
    conf = fixture()
    for token in conf["revoked"]:
        with client(token) as runtime:
            denied(
                lambda c=runtime: c.events.publish(
                    {"event_type": "sdk:isolation", "event_data": {}}
                ),
                (401,),
            )
    for token in conf["narrowed"]:
        with client(token) as runtime:
            runtime.events.publish({"event_type": "sdk:isolation", "event_data": {}})
            denied(runtime.projects.list, (403,))
            denied(
                lambda c=runtime: c.events.publish(
                    {"event_type": "other:blocked", "event_data": {}}
                ),
                (403,),
            )


def test_async_org_api_and_runtime():
    conf = fixture()

    async def run():
        base_url = os.environ["HOT_TEST_BASE_URL"]
        async with AsyncHotClient(token=conf["org_token"], base_url=base_url) as org:
            assert isinstance((await org.orgs.list())["data"], list)
            usage = await org.orgs.usage(conf["root_org_id"])
            assert usage["usage_scope"] == "account"
            with pytest.raises(HotKeyScopeError) as error:
                await org.env.get()
            assert error.value.required_scope == "env"
        async with AsyncHotClient(token=conf["narrowed"][0], base_url=base_url) as runtime:
            await runtime.events.publish({"event_type": "sdk:isolation", "event_data": {}})
            with pytest.raises(HotApiError) as error:
                await runtime.projects.list()
            assert error.value.status_code == 403
            with pytest.raises(HotKeyScopeError) as error:
                await runtime.orgs.get(conf["root_org_id"])
            assert error.value.required_scope == "org"

    asyncio.run(run())
