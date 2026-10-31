# hot-dev

Official Python SDK for the [Hot Dev](https://hot.dev) API.

- PyPI distribution: `hot-dev`
- Import package: `hot`
- Tested on Python 3.10 through 3.15

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
terminal snapshot. `timeout` bounds the whole wait, including reconnects:

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
To coordinate several tasks, wait on each one (`hot.tasks.wait`), for example
from a thread pool or with `asyncio.gather` on `AsyncHotClient`. The task's
parent stream also emits durable `task:update` events, but a plain
`streams.subscribe` ends at the API's 5-minute subscription limit.

### Waiters and reconnects

`runs.wait`, `tasks.wait`, `streams.wait_for_run_result`, `events.call_hot`
and `streams.subscribe_with_event` share one reconnect policy:

- `timeout` is an absolute deadline over connecting, reading, and backoff. It
  fires even while the server keeps sending keepalives. `subscribe_with_event`
  takes an optional `timeout`; without one it has no deadline.
- Permanent errors (any 4xx except 408 and 429, including `HotKeyScopeError`)
  raise at once.
- Transport errors, 408, 429 and 5xx reconnect with jittered exponential
  backoff (0.25 s up to 10 s), honouring `retry_after`. A backoff that would
  cross the deadline raises `TimeoutError` instead.
- `max_attempts` (default 5) counts consecutive failed connections. The API
  closes every subscription 5 minutes after it opens, with `stream:complete`;
  those rollovers reconnect at once and do not count, so long runs can be
  awaited for as long as `timeout` allows.
- A run that fails with status `pending_retry` is not final: the waiter
  follows the handler's retry run (linked by `origin_run_id`).
- `on_chunk` receives `stream:data` text in order, including chunks the server
  delivers before `run:start`.

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

A call made with the wrong kind of key raises `HotKeyScopeError` (a
`HotApiError` with status 403 and code `key_scope_mismatch`). Its
`required_scope` is `"org"` for `hot.orgs` and `hot.api_keys`, and `"env"` for
every runtime resource.

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`, and idempotent requests (GET, PUT, DELETE) also on
502, 503 and 504. Other errors are raised as-is. Raw requests are never
retried; waiters follow the reconnect policy above.

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `hot.events` — publish, list, get, inspect event runs, and call Hot functions with `call_hot(fn, args)`
- `hot.streams` — subscribe to run and task updates, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; pass `reconnect=False` to opt out)
- `hot.runs` — list, inspect, subscribe to, and wait for durable runs
- `hot.tasks` — get, subscribe to, and wait for durable background tasks
- `hot.files` — upload, download (`download_stream` for large files), list, and delete files (including multipart uploads)
- `hot.projects` — create, list, update, activate, deactivate, and delete projects
- `hot.builds` — upload, download (`download_stream`), deploy, and look up live/deployed builds
- `hot.context` — manage encrypted project context variables
- `hot.domains` — register, verify, list, and delete custom domains
- `hot.sessions` — create and revoke scoped sessions
- `hot.service_keys` — create and revoke scoped service keys
- `hot.org` — view the environment's usage and limits
- `hot.env` — read environment info, subscribe to environment events, and purge call traces
- `hot.orgs` — with an org API key: child organizations, their environments and API keys, account usage, and `orgs.env(org_id, env_id)` for projects, builds and context in an environment
- `hot.api_keys` — with an org API key: get, narrow, rotate, and revoke API keys it issued

Use `hot.request(...)` or `hot.request_raw(...)` as an escape hatch for API
endpoints that do not yet have a resource helper.

`hot.env.subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503. `subscribe_with_event`
reconnects across the API's 5-minute SSE timeout and stops after the terminal
run correlated to the event it published. Unrelated runs on the same stream do
not end the iterator.

## Customizing the HTTP Client

Without `timeout` or `client`, requests time out after 60 s per read and 10 s
to connect; pass `timeout=httpx.Timeout(None)` to disable that. Streams and
waiters set their own timeouts.

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

## Downloads

`files.download` and `builds.download` read the whole body into memory. For
large files, stream instead:

```python
with hot.files.download_stream(file_id) as response, open("out.bin", "wb") as out:
    for chunk in response.iter_bytes():
        out.write(chunk)
```

`AsyncHotClient` offers the same with `async with` and `response.aiter_bytes()`.

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

## Org API

Requires Hot 3.0.1 or later. An org API key (created by a root admin in the
App) provisions child organizations, their environments, and environment API
keys. Org keys only call `hot.orgs` and `hot.api_keys`; environment keys,
service keys, and sessions only call runtime resources. Either kind of key on
the wrong routes raises `HotKeyScopeError`.

```python
import os
import uuid

from hot import HotClient

with HotClient(token=os.environ["HOT_ORG_API_KEY"]) as org:
    request_id = uuid.uuid4().hex[:24]
    child = org.orgs.create(
        {
            "name": "Customer",
            "slug": f"customer-{request_id}",  # 32 characters or fewer
            "external_id": "customer-123",
            "idempotency_key": request_id,  # reuse it when retrying this request
        }
    )
    org_id, env_id = child["org"]["org_id"], child["env_id"]
    key = org.orgs.create_env_api_key(
        org_id,
        env_id,
        {"description": "Customer backend", "permissions": {"*:*": ["*"]}},
    )
    # Projects, builds and context in the child's environment, with the org key.
    org.orgs.env(org_id, env_id).projects.create({"name": "agent"})

with HotClient(token=key["token"]) as customer:
    print(customer.env.get())
```

| Method | Route |
| --- | --- |
| `orgs.list(limit=, offset=)` | `GET /v1/orgs` (list envelope) |
| `orgs.create(body)` | `POST /v1/orgs`: returns `org`, `env_id`, `created` |
| `orgs.get(org_id)` / `orgs.update(org_id, {"state": ...})` | `GET`/`PATCH /v1/orgs/{org_id}` (`active`, `suspended`, `archived`) |
| `orgs.usage(root_org_id)` | `GET /v1/orgs/{org_id}/usage` (account usage; root org only) |
| `orgs.purge_calls(org_id, include_sub_orgs=False)` | `DELETE /v1/orgs/{org_id}/calls` |
| `orgs.list_envs` / `create_env` / `get_env` | `/v1/orgs/{org_id}/envs[/{env_id}]` (create is idempotent by name) |
| `orgs.list_api_keys` / `create_api_key` | `/v1/orgs/{org_id}/api-keys` (org keys) |
| `orgs.list_env_api_keys` / `create_env_api_key` | `/v1/orgs/{org_id}/envs/{env_id}/api-keys` |
| `api_keys.get` / `update` / `revoke` / `rotate(overlap_seconds=)` | `/v1/api-keys/{key_id}[/rotate]` |
| `orgs.env(org_id, env_id).projects` / `.builds` / `.context` | `/v1/orgs/{org_id}/envs/{env_id}/...` |

A minted or rotated key's `token` is returned only once. Minted grants must fit
inside the org key's own grants for that environment. Revoking or narrowing an
org key restricts every key, service key, and session issued under it.

`hot.org.usage()` (runtime) reports one environment's consumption, with
`env_id` and `usage_scope: "environment"`; its limits come from the billing
account. Use `orgs.usage(root_org_id)` with an org key for the account total.

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
ruff check .
pytest
python -m build
```

## Org API integration tests

`integration/suborg_fixture.py` creates an owned disposable database and
loopback API server, seeds an org API key and a root environment key, checks
the live OpenAPI operations against `tests/openapi_operations.json`, and runs
the given test command. It removes only its own resources and never uses
deployed credentials. It needs Python 3 and a Hot CLI built from the revision
in `integration/hot-server.ref`. For PostgreSQL, also install Docker and `psql`
and provide that hot checkout (default: sibling `hot`). Migrations use a
non-superuser schema owner; the API uses a distinct non-owner
`NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  .venv/bin/python -m pytest tests/test_suborg_integration.py -v
# For SQLite, use --backend sqlite.
```

Coverage includes child and environment provisioning and idempotency, identical
project names in siblings, foreign-ID and stream denial, root runtime
isolation, org/environment key separation (`HotKeyScopeError`), account and
environment usage, provisioning projects and context through `orgs.env`,
restricted keys, key rotation, narrowing and revocation, service/session
issuance and revocation, descendants of a revoked or narrowed org key, and child
suspension and resumption. See [the validation record](integration/VALIDATION.md).

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Hot Chat demo](https://github.com/hot-dev/hot-demos/tree/main/hot-chat)

## License

Apache-2.0
