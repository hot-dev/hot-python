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

## Errors

Non-2xx API responses raise `HotApiError` with structured fields:

```python
from hot import HotApiError

try:
    hot.projects.get("missing-project")
except HotApiError as error:
    print(error.status_code, error.code, error.request_id)
```

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `hot.events` — publish, list, get, and inspect event runs
- `hot.streams` — subscribe to run streams and publish events atomically (reconnects automatically across the 5-minute SSE timeout; pass `reconnect=False` to opt out)
- `hot.runs` — list, inspect, and view run stats
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
- [Python SDK docs](https://hot.dev/docs/api/python)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Hot Chat demo](https://github.com/hot-dev/hot-demos/tree/main/hot-chat)

## License

Apache-2.0
