# hot-dev

Official Python SDK for the [Hot Dev](https://hot.dev) API.

- PyPI distribution: `hot-dev`
- Import package: `hot`
- Tested on Python 3.10, 3.11, 3.12, 3.13

## Install

```bash
pip install hot-dev
```

## Quick Start

```python
import os

from hot import HotClient

hot = HotClient(token=os.environ["HOT_API_KEY"])

# base_url defaults to https://api.hot.dev.
# For local development with `hot dev`, pass base_url="http://localhost:4681".

for event in hot.streams.subscribe_with_event(
    {
        "event_type": "team-agent:ask",
        "event_data": {
            "session_id": "web:chat:demo",
            "user_id": "web:user:demo",
            "user_name": "Demo User",
            "question": "what is blocking launch?",
        },
    }
):
    if event["type"] == "stream:data":
        print(event["data_type"], event.get("payload"))

    if event["type"] == "run:stop":
        print(event.get("run", {}).get("result"))
        break
```

The client closes its underlying HTTP connection cleanly when used as a context
manager:

```python
with HotClient(token=os.environ["HOT_API_KEY"]) as hot:
    event = hot.events.publish(
        {
            "event_type": "team-agent:ask",
            "event_data": {"question": "what changed?"},
        }
    )
    print(event["stream_id"])
```

Authenticated clients should run server-side. Browser apps and public notebooks
should call your own backend route instead of exposing a Hot API key.
Administrative runtime endpoints require an API key with appropriate grants. Sessions and service keys are
permission-scoped and are mainly for event publishing and stream reads.

When code has a run id but does not need the full stream, wait on its durable
terminal snapshot:

```python
run = hot.runs.wait(run_id, timeout=300)
print(run["result"])
```

When a run returns a background task id, wait through the durable task resource:

```python
task = hot.tasks.wait(task_id, timeout=300)
print(task["result"])
```

The task waiter receives the latest persisted state first and reconnects, so it
cannot miss a task that completed before subscription. A failed, cancelled, or
timed-out task raises `HotTaskError` with the final task record.
The task's parent stream also emits durable `task:update` events, which is
useful when one subscription needs to coordinate several tasks.

## Async Usage

```python
import os

from hot import AsyncHotClient

async with AsyncHotClient(token=os.environ["HOT_API_KEY"]) as hot:
    event = await hot.events.publish(
        {
            "event_type": "team-agent:ask",
            "event_data": {"question": "what changed?"},
        }
    )
    print(event["stream_id"])
```

Async streaming is supported the same way:

```python
async with AsyncHotClient(token=os.environ["HOT_API_KEY"]) as hot:
    async for event in hot.streams.subscribe_with_event(
        {
            "event_type": "team-agent:ask",
            "event_data": {"question": "what changed?"},
        }
    ):
        print(event["type"], event)
```

`AsyncHotClient` exposes the same resource namespaces as `HotClient`.

## Errors

Non-2xx API responses raise `HotApiError` with structured fields:

```python
from hot import HotApiError

try:
    hot.projects.get("missing-project")
except HotApiError as error:
    print(error.status_code, error.code, error.request_id, error.retry_after)
```

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`; other errors are raised as-is. Streaming and raw
requests are never retried.

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `hot.events` — publish, list, get, inspect event runs, and call Hot functions with `call_hot(fn, args)`
- `hot.streams` — subscribe to run and task updates, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; pass `reconnect=False` to opt out)
- `hot.runs` — list, inspect, subscribe to, and wait for durable runs
- `hot.tasks` — get, subscribe to, and wait for durable background tasks
- `hot.files` — upload, download, list, and delete files (including multipart uploads)
- `hot.projects` — create, list, update, activate, deactivate, and delete projects
- `hot.builds` — upload, download, deploy, and look up live/deployed builds
- `hot.context` — manage encrypted project context variables
- `hot.domains` — register, verify, list, and delete custom domains
- `hot.sessions` — create and revoke scoped sessions
- `hot.service_keys` — create and revoke scoped service keys
- `hot.org` — view usage and limits
- `hot.env` — read environment info and subscribe to environment events

Use `hot.request(...)` or `hot.request_raw(...)` as an escape hatch for API
endpoints that do not yet have a resource helper.

`hot.env.subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503. `subscribe_with_event`
reconnects across the API's 5-minute SSE timeout and stops after the terminal
run correlated to the event it published. Unrelated runs on the same stream do
not end the iterator.

## Customizing the HTTP Client

`HotClient` and `AsyncHotClient` accept an `httpx.Client` (or
`httpx.AsyncClient`) via the `client=` argument. Use this to configure retries,
proxies, or custom transports:

```python
import httpx

from hot import HotClient

http_client = httpx.Client(
    timeout=httpx.Timeout(connect=5.0, read=60.0, write=30.0, pool=5.0),
    transport=httpx.HTTPTransport(retries=3),
)

hot = HotClient(token=os.environ["HOT_API_KEY"], client=http_client)
```

When you pass your own client, the SDK does not close it on exit.

## Casing Policy

Core API request and response payloads use the Hot API wire format:
`event_type`, `event_data`, `stream_id`, and so on. SDK-only options use Python
style names such as `base_url` and `timeout`.

The SDK never transforms user-owned payloads such as `event_data`.

## Type Hints

The package ships with `py.typed`, so editors and type checkers will pick up
the public API. Request and response payloads are currently typed as plain
`dict[str, Any]` — keys follow the Hot API wire format documented above. Strict
`TypedDict` shapes generated from the Hot API OpenAPI spec are planned for a
future release.

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
ruff check .
pytest
python -m build
```

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Hot Chat demo](https://github.com/hot-dev/hot-demos/tree/main/hot-chat)

## License

Apache-2.0

## Sub-organization management

Requires a Hot server release containing the sub-org management API. Use the
separate `HotManagementClient` with an `hm_` credential issued by a root admin.
Runtime API keys, service keys, and sessions cannot call management endpoints;
management credentials cannot call runtime endpoints. Create a runtime client
per environment credential. A root environment key cannot access its children,
and a child key cannot access siblings. There is no tenant or org selector on
runtime methods. Each child owns its environments and deployments.

```python
from hot import HotClient, HotManagementClient

with HotManagementClient(token=management_token) as management:
    child = management.create_child({
        "name": "Customer", "slug": "customer-unique-slug",
        "idempotency_key": "customer-provisioning-123",
    })
    key = management.create_api_key(child["org"]["org_id"], child["env_id"], {
        "description": "Customer backend", "permissions": {"*:*": ["*"]},
    })
with HotClient(token=key["token"]) as customer:
    print(customer.env.get())
```

`AsyncHotManagementClient` provides the same methods with `await` and `async with`.

| Management method | Purpose |
| --- | --- |
| `list_children(limit=20, offset=0)` | List authorized children (returns `children` and `total`) |
| `create_child(body)` | Provision a child and default environment |
| `set_child_state(child_id, state)` | Set `active`, `suspended`, or `archived` (no response body) |
| `create_environment(child_id, body)` | Provision a child environment by name |
| `create_api_key(child_id, env_id, body)` | Issue a child API key within the delegation ceiling |
| `usage()` | Read account-wide usage |

Child creation requires a stable `idempotency_key`; reuse it for retries of the
same request. A different payload with that key is rejected. Environment
creation is idempotent within a child by exact name. Listing accepts `limit`
(maximum 500) and `offset`. Account usage requires `usage:read` and explicit
`all_children` authority. Management credentials cannot issue other management
credentials or grant human access. Service keys and sessions are issued with the
child environment's runtime API key and inherit its management issuer chain.
Issuer expiry, revocation, or permission narrowing restrict existing descendants;
expansion never expands their stored grants.

Administrative runtime operations require an API key **and** the appropriate
resource/action grants. For example, project listing requires `project:*` with
`read`, and environment info and usage require `env:{env_id}` with `read`.
Event-only keys receive `403` for these operations; API-key status alone does
not grant administration. Full-access `{"*:*": ["*"]}` keys retain access.
See the server's [permission contract](https://hot.dev/docs/api/management).

`org.usage` reports only the credential's **environment consumption**, with
`env_id`, `usage_scope: "environment"`, and `limits_scope: "billing_account"`.
Its limits and plan come from the billing root; percentages compare this one
environment against shared limits and do not describe remaining account quota.
`usage.team_members` is zero because seats belong to the account. Use management
`usage` for the account aggregate. Existing runtime endpoints and request bodies
keep their shapes. JSON requests retain bounded 429/Retry-After handling;
streaming reconnects and waiters stop on permanent `401`/`403` authorization errors.

### Sub-org integration tests

See [the validation record](integration/VALIDATION.md) for the tested server revision and results.

The fixture creates an owned disposable database and loopback API server, runs
the SDK tests, and removes only its own resources. The canonical fixture lives
in hot-python and is synchronized across all five SDKs. No deployed credentials
are used. Requires Python 3 and a Hot CLI built from the matching server branch.
For PostgreSQL, also install Docker and `psql` and provide the public hot checkout
(default: sibling `hot`). Migrations use a non-superuser schema owner; the API
uses a distinct non-owner `NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.
The fixture's administrator seeds credentials only; SDK calls always use HTTP.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  .venv/bin/python -m pytest tests/test_suborg_integration.py -v
# For SQLite, use --backend sqlite.
```

Coverage includes provisioning and environment idempotency, identical project
names in siblings, foreign-ID and stream denial, parent runtime isolation,
management/runtime token separation, usage scope, restricted API-key grants,
service/session issuance and revocation, pre-existing descendant issuer
revocation/narrowing, and child suspension/resumption.
