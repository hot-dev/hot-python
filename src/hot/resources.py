from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any
from urllib.parse import quote

from ._transport import AsyncTransport, SyncTransport, raise_for_status, without_none
from .sse import consume_sse_blocks
from .types import StreamEvent


def enc(value: str) -> str:
    return quote(value, safe="")


def enc_path(path: str) -> str:
    return "/".join(quote(part, safe="") for part in path.split("/"))


def _run_id_of(event: Any) -> str | None:
    if not isinstance(event, dict):
        return None
    run = event.get("run")
    if isinstance(run, dict):
        run_id = run.get("run_id")
        if isinstance(run_id, str):
            return run_id
    return None


def _event_id_of_run(run: Any) -> str | None:
    if not isinstance(run, dict):
        return None
    event_id = run.get("event_id")
    return event_id if isinstance(event_id, str) else None


def _result_message(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if not isinstance(value, dict):
        return None
    for key in ("$err", "error", "message", "reason"):
        message = _result_message(value.get(key))
        if message:
            return message
    return None


def _run_result_message(run: Mapping[str, Any], fallback: str) -> str:
    message = _result_message(run.get("result"))
    if message:
        return message
    status = run.get("status")
    return f"{fallback} ({status})" if isinstance(status, str) and status else fallback


def _extract_run_result(result: Any) -> Any:
    if isinstance(result, dict) and "$ok" in result:
        return result["$ok"]
    return result


def _emit_chunk(event: StreamEvent, on_chunk: Callable[[str], None] | None) -> None:
    if not on_chunk or event.get("type") != "stream:data":
        return
    payload = event.get("payload")
    if isinstance(payload, dict):
        text = payload.get("text")
        if isinstance(text, str):
            on_chunk(text)


class EventsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def publish(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/events", json=dict(body))["data"]

    def call_hot(
        self,
        fn: str,
        args: list[Any] | None = None,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
    ) -> Any:
        published = self.publish(
            {
                "event_type": "hot:call",
                "event_data": {
                    "fn": fn,
                    "args": args or [],
                },
            }
        )
        run = wait_for_run_result(
            self._transport,
            published["stream_id"],
            published["event_id"],
            timeout=timeout,
            on_chunk=on_chunk,
        )
        return _extract_run_result(run.get("result"))

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/events", params=query)

    def get(self, event_id: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/events/{enc(event_id)}")["data"]

    def get_runs(self, event_id: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/events/{enc(event_id)}/runs")


class RunsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/runs", params=query)

    def stats(self) -> dict[str, Any]:
        return self._transport.request_json("GET", "/runs/stats")["data"]

    def get(self, run_id: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/runs/{enc(run_id)}")["data"]


class EnvResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self) -> dict[str, Any]:
        return self._transport.request_json("GET", "/env")["data"]

    def subscribe(self) -> Iterator[StreamEvent]:
        yield from iter_stream(self._transport, "GET", "/env/subscribe")


class OrgResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def usage(self) -> dict[str, Any]:
        return self._transport.request_json("GET", "/org/usage")["data"]


class StreamsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def subscribe(self, stream_id: str, *, project: str | None = None) -> Iterator[StreamEvent]:
        yield from iter_stream(
            self._transport,
            "GET",
            f"/streams/{enc(stream_id)}/subscribe",
            params={"project": project},
        )

    def subscribe_post(
        self,
        stream_id: str,
        *,
        project: str | None = None,
    ) -> Iterator[StreamEvent]:
        yield from iter_stream(
            self._transport,
            "POST",
            f"/streams/{enc(stream_id)}/subscribe",
            params={"project": project},
        )

    def wait_for_run_result(
        self,
        stream_id: str,
        event_id: str,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        return wait_for_run_result(
            self._transport,
            stream_id,
            event_id,
            timeout=timeout,
            on_chunk=on_chunk,
        )

    def subscribe_with_event(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        reconnect: bool = True,
        max_attempts: int = 5,
    ) -> Iterator[StreamEvent]:
        """Atomically subscribe then publish.

        The Hot API closes SSE subscriptions after 5 minutes of idle time. When
        ``reconnect=True`` (the default), the SDK resubscribes to the same
        ``stream_id`` and continues yielding events until a terminal
        ``run:stop``/``run:fail``/``run:cancel`` arrives. Replayed
        ``run:start`` and terminal events are deduped by ``run_id``.
        ``stream:data`` chunks are not replayed and may be lost across the
        disconnect window.

        Pass ``reconnect=False`` for the 1.0.x single-connection behavior.
        """
        if not reconnect:
            yield from self._subscribe_with_event_once(body, project=project)
            return

        seen_start: set[str] = set()
        seen_terminal: set[str] = set()
        stream_id: str | None = None
        attempts = 0

        while True:
            if stream_id is None:
                iterator = self._subscribe_with_event_once(body, project=project)
            else:
                iterator = self.subscribe(stream_id, project=project)

            terminal = False
            try:
                for event in iterator:
                    event_type = event.get("type") if isinstance(event, dict) else None

                    if event_type == "event:published":
                        published = event.get("stream_id") if isinstance(event, dict) else None
                        if isinstance(published, str):
                            stream_id = published

                    if event_type == "run:start":
                        run_id = _run_id_of(event)
                        if run_id and run_id in seen_start:
                            continue
                        if run_id:
                            seen_start.add(run_id)

                    if event_type in ("run:stop", "run:fail", "run:cancel"):
                        run_id = _run_id_of(event)
                        if run_id and run_id in seen_terminal:
                            continue
                        if run_id:
                            seen_terminal.add(run_id)
                        terminal = True

                    yield event
            except Exception:
                if stream_id is None or attempts >= max_attempts:
                    raise

            if terminal:
                return
            if stream_id is None:
                raise RuntimeError("stream ended before event:published was received")

            attempts += 1
            time.sleep(min(0.25 * attempts, 2.0))

    def _subscribe_with_event_once(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
    ) -> Iterator[StreamEvent]:
        yield from iter_stream(
            self._transport,
            "POST",
            "/streams/subscribe-with-event",
            json=dict(body),
            params={"project": project},
        )


class FilesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/files", params=query)

    def get(self, file_id: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/files/{enc(file_id)}")["data"]

    def delete(self, file_id: str) -> None:
        self._transport.request_raw("DELETE", f"/files/{enc(file_id)}")

    def download(self, file_id: str):
        return self._transport.request_raw("GET", f"/files/{enc(file_id)}/download")

    def upload(
        self,
        path: str,
        content: bytes | str,
        *,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        response = self._transport.request_raw(
            "PUT",
            f"/files/upload/{enc_path(path)}",
            content=content,
            headers={
                "Accept": "application/json",
                **({"Content-Type": content_type} if content_type else {}),
            },
        )
        return response.json()["data"]

    def initiate_upload(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/files/uploads", json=dict(body))["data"]

    def upload_part(self, upload_id: str, part_number: int, content: bytes | str) -> dict[str, Any]:
        response = self._transport.request_raw(
            "PUT",
            f"/files/uploads/{enc(upload_id)}/{part_number}",
            content=content,
            headers={"Accept": "application/json"},
        )
        return response.json()["data"]

    def complete_upload(self, upload_id: str) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/files/uploads/{enc(upload_id)}/complete",
        )["data"]

    def abort_upload(self, upload_id: str) -> None:
        self._transport.request_raw("DELETE", f"/files/uploads/{enc(upload_id)}")


class ProjectsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/projects", params=query)

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/projects", json=dict(body))["data"]

    def get(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/projects/{enc(project)}")["data"]

    def update(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "PATCH",
            f"/projects/{enc(project)}",
            json=dict(body),
        )["data"]

    def delete(self, project: str) -> None:
        self._transport.request_raw("DELETE", f"/projects/{enc(project)}")

    def activate(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("POST", f"/projects/{enc(project)}/activate")["data"]

    def deactivate(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("POST", f"/projects/{enc(project)}/deactivate")[
            "data"
        ]

    def event_handlers(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/projects/{enc(project)}/event-handlers")

    def schedules(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/projects/{enc(project)}/schedules")


class BuildsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/builds", params=query)

    def list_for_project(self, project: str, **query: Any) -> dict[str, Any]:
        return self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/builds",
            params=query,
        )

    def get(self, project: str, build_id: str) -> dict[str, Any]:
        return self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/builds/{enc(build_id)}",
        )["data"]

    def deployed(self, project: str) -> dict[str, Any]:
        return self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/builds/deployed",
        )["data"]

    def live(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/projects/{enc(project)}/builds/live")[
            "data"
        ]

    def upload(
        self,
        project: str,
        *,
        file: Any,
        hash: str,
        build_id: str | None = None,
    ) -> dict[str, Any]:
        data = {"hash": hash, **({"build_id": build_id} if build_id else {})}
        response = self._transport.request_raw(
            "POST",
            f"/projects/{enc(project)}/builds",
            data=data,
            files={"file": file},
            headers={"Accept": "application/json"},
        )
        return response.json()["data"]

    def download(self, project: str, build_id: str):
        return self._transport.request_raw(
            "GET",
            f"/projects/{enc(project)}/builds/{enc(build_id)}/download",
        )

    def deploy(self, project: str, build_id: str) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/projects/{enc(project)}/builds/{enc(build_id)}/deploy",
        )["data"]


class ContextResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, project: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/projects/{enc(project)}/context")

    def create(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/projects/{enc(project)}/context",
            json=dict(body),
        )["data"]

    def update(self, project: str, key: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "PUT",
            f"/projects/{enc(project)}/context/{enc(key)}",
            json=dict(body),
        )["data"]

    def delete(self, project: str, key: str) -> None:
        self._transport.request_raw("DELETE", f"/projects/{enc(project)}/context/{enc(key)}")


class DomainsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/domains", json=dict(body))["data"]

    def list(self) -> dict[str, Any]:
        return self._transport.request_json("GET", "/domains")

    def get(self, domain_id: str) -> dict[str, Any]:
        return self._transport.request_json("GET", f"/domains/{enc(domain_id)}")["data"]

    def delete(self, domain_id: str) -> None:
        self._transport.request_raw("DELETE", f"/domains/{enc(domain_id)}")

    def verify(self, domain_id: str) -> dict[str, Any]:
        return self._transport.request_json("POST", f"/domains/{enc(domain_id)}/verify")["data"]


class SessionsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/sessions", json=dict(body))["data"]

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/sessions", params=query)

    def revoke(self, session_id: str) -> None:
        self._transport.request_raw("DELETE", f"/sessions/{enc(session_id)}")

    def revoke_all(self) -> dict[str, Any]:
        return self._transport.request_json("DELETE", "/sessions")["data"]


class ServiceKeysResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json("POST", "/service-keys", json=dict(body))["data"]

    def list(self, **query: Any) -> dict[str, Any]:
        return self._transport.request_json("GET", "/service-keys", params=query)

    def get(self, service_key_id: str) -> dict[str, Any]:
        return self._transport.request_json(
            "GET",
            f"/service-keys/{enc(service_key_id)}",
        )["data"]

    def update(self, service_key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "PATCH",
            f"/service-keys/{enc(service_key_id)}",
            json=dict(body),
        )["data"]

    def revoke(self, service_key_id: str) -> None:
        self._transport.request_raw("DELETE", f"/service-keys/{enc(service_key_id)}")

    def revoke_all(self) -> dict[str, Any]:
        return self._transport.request_json("DELETE", "/service-keys")["data"]


def iter_stream(
    transport: SyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
) -> Iterator[StreamEvent]:
    with transport.client.stream(
        method,
        f"{transport.api_base_url}{path}",
        headers=transport.headers(
            {
                "Accept": "text/event-stream",
                **({"Content-Type": "application/json"} if json is not None else {}),
            }
        ),
        json=dict(json) if json is not None else None,
        params=without_none(params),
    ) as response:
        raise_for_status(response)
        buffer = ""
        for chunk in response.iter_text():
            buffer += chunk
            events, buffer = consume_sse_blocks(buffer)
            yield from events
        events, _ = consume_sse_blocks(f"{buffer}\n\n")
        yield from events


def wait_for_run_result(
    transport: SyncTransport,
    stream_id: str,
    event_id: str,
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    current_run_id: str | None = None

    for event in iter_stream(transport, "GET", f"/streams/{enc(stream_id)}/subscribe"):
        if time.monotonic() > deadline:
            raise TimeoutError("Timeout waiting for run result")

        if event.get("type") == "run:start":
            run = event.get("run")
            if _event_id_of_run(run) == event_id:
                current_run_id = _run_id_of(event)

        if current_run_id and event.get("type") == "stream:data":
            if event.get("run_id") == current_run_id:
                _emit_chunk(event, on_chunk)

        if event.get("type") in ("run:stop", "run:fail", "run:cancel"):
            run = event.get("run")
            if not isinstance(run, dict) or _event_id_of_run(run) != event_id:
                continue
            if event.get("type") == "run:fail":
                raise RuntimeError(_run_result_message(run, "Run failed"))
            if event.get("type") == "run:cancel":
                raise RuntimeError(_run_result_message(run, "Run cancelled"))
            return run

    raise RuntimeError("Stream ended before run completed")


async def async_wait_for_run_result(
    transport: AsyncTransport,
    stream_id: str,
    event_id: str,
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    current_run_id: str | None = None

    async for event in aiter_stream(transport, "GET", f"/streams/{enc(stream_id)}/subscribe"):
        if time.monotonic() > deadline:
            raise TimeoutError("Timeout waiting for run result")

        if event.get("type") == "run:start":
            run = event.get("run")
            if _event_id_of_run(run) == event_id:
                current_run_id = _run_id_of(event)

        if current_run_id and event.get("type") == "stream:data":
            if event.get("run_id") == current_run_id:
                _emit_chunk(event, on_chunk)

        if event.get("type") in ("run:stop", "run:fail", "run:cancel"):
            run = event.get("run")
            if not isinstance(run, dict) or _event_id_of_run(run) != event_id:
                continue
            if event.get("type") == "run:fail":
                raise RuntimeError(_run_result_message(run, "Run failed"))
            if event.get("type") == "run:cancel":
                raise RuntimeError(_run_result_message(run, "Run cancelled"))
            return run

    raise RuntimeError("Stream ended before run completed")


class AsyncEventsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def publish(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (await self._transport.request_json("POST", "/events", json=dict(body)))["data"]

    async def call_hot(
        self,
        fn: str,
        args: list[Any] | None = None,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
    ) -> Any:
        published = await self.publish(
            {
                "event_type": "hot:call",
                "event_data": {
                    "fn": fn,
                    "args": args or [],
                },
            }
        )
        run = await async_wait_for_run_result(
            self._transport,
            published["stream_id"],
            published["event_id"],
            timeout=timeout,
            on_chunk=on_chunk,
        )
        return _extract_run_result(run.get("result"))

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/events", params=query)

    async def get(self, event_id: str) -> dict[str, Any]:
        return (await self._transport.request_json("GET", f"/events/{enc(event_id)}"))["data"]

    async def get_runs(self, event_id: str) -> dict[str, Any]:
        return await self._transport.request_json("GET", f"/events/{enc(event_id)}/runs")


class AsyncRunsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/runs", params=query)

    async def stats(self) -> dict[str, Any]:
        return (await self._transport.request_json("GET", "/runs/stats"))["data"]

    async def get(self, run_id: str) -> dict[str, Any]:
        return (await self._transport.request_json("GET", f"/runs/{enc(run_id)}"))["data"]


class AsyncEnvResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self) -> dict[str, Any]:
        return (await self._transport.request_json("GET", "/env"))["data"]

    async def subscribe(self) -> AsyncIterator[StreamEvent]:
        async for event in aiter_stream(self._transport, "GET", "/env/subscribe"):
            yield event


class AsyncOrgResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def usage(self) -> dict[str, Any]:
        return (await self._transport.request_json("GET", "/org/usage"))["data"]


class AsyncStreamsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def subscribe(
        self,
        stream_id: str,
        *,
        project: str | None = None,
    ) -> AsyncIterator[StreamEvent]:
        async for event in aiter_stream(
            self._transport,
            "GET",
            f"/streams/{enc(stream_id)}/subscribe",
            params={"project": project},
        ):
            yield event

    async def subscribe_post(
        self,
        stream_id: str,
        *,
        project: str | None = None,
    ) -> AsyncIterator[StreamEvent]:
        async for event in aiter_stream(
            self._transport,
            "POST",
            f"/streams/{enc(stream_id)}/subscribe",
            params={"project": project},
        ):
            yield event

    async def wait_for_run_result(
        self,
        stream_id: str,
        event_id: str,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        return await async_wait_for_run_result(
            self._transport,
            stream_id,
            event_id,
            timeout=timeout,
            on_chunk=on_chunk,
        )

    async def subscribe_with_event(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        reconnect: bool = True,
        max_attempts: int = 5,
    ) -> AsyncIterator[StreamEvent]:
        """Async variant of :meth:`StreamsResource.subscribe_with_event`."""
        if not reconnect:
            async for event in self._subscribe_with_event_once(body, project=project):
                yield event
            return

        seen_start: set[str] = set()
        seen_terminal: set[str] = set()
        stream_id: str | None = None
        attempts = 0

        while True:
            if stream_id is None:
                source = self._subscribe_with_event_once(body, project=project)
            else:
                source = self.subscribe(stream_id, project=project)

            terminal = False
            try:
                async for event in source:
                    event_type = event.get("type") if isinstance(event, dict) else None

                    if event_type == "event:published":
                        published = event.get("stream_id") if isinstance(event, dict) else None
                        if isinstance(published, str):
                            stream_id = published

                    if event_type == "run:start":
                        run_id = _run_id_of(event)
                        if run_id and run_id in seen_start:
                            continue
                        if run_id:
                            seen_start.add(run_id)

                    if event_type in ("run:stop", "run:fail", "run:cancel"):
                        run_id = _run_id_of(event)
                        if run_id and run_id in seen_terminal:
                            continue
                        if run_id:
                            seen_terminal.add(run_id)
                        terminal = True

                    yield event
            except Exception:
                if stream_id is None or attempts >= max_attempts:
                    raise

            if terminal:
                return
            if stream_id is None:
                raise RuntimeError("stream ended before event:published was received")

            attempts += 1
            await asyncio.sleep(min(0.25 * attempts, 2.0))

    async def _subscribe_with_event_once(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
    ) -> AsyncIterator[StreamEvent]:
        async for event in aiter_stream(
            self._transport,
            "POST",
            "/streams/subscribe-with-event",
            json=dict(body),
            params={"project": project},
        ):
            yield event


class AsyncFilesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/files", params=query)

    async def get(self, file_id: str) -> dict[str, Any]:
        return (await self._transport.request_json("GET", f"/files/{enc(file_id)}"))["data"]

    async def delete(self, file_id: str) -> None:
        await self._transport.request_raw("DELETE", f"/files/{enc(file_id)}")

    async def download(self, file_id: str):
        return await self._transport.request_raw("GET", f"/files/{enc(file_id)}/download")

    async def upload(
        self,
        path: str,
        content: bytes | str,
        *,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        response = await self._transport.request_raw(
            "PUT",
            f"/files/upload/{enc_path(path)}",
            content=content,
            headers={
                "Accept": "application/json",
                **({"Content-Type": content_type} if content_type else {}),
            },
        )
        return response.json()["data"]

    async def initiate_upload(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (await self._transport.request_json("POST", "/files/uploads", json=dict(body)))[
            "data"
        ]

    async def upload_part(
        self,
        upload_id: str,
        part_number: int,
        content: bytes | str,
    ) -> dict[str, Any]:
        response = await self._transport.request_raw(
            "PUT",
            f"/files/uploads/{enc(upload_id)}/{part_number}",
            content=content,
            headers={"Accept": "application/json"},
        )
        return response.json()["data"]

    async def complete_upload(self, upload_id: str) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "POST",
                f"/files/uploads/{enc(upload_id)}/complete",
            )
        )["data"]

    async def abort_upload(self, upload_id: str) -> None:
        await self._transport.request_raw("DELETE", f"/files/uploads/{enc(upload_id)}")


class AsyncProjectsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/projects", params=query)

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (await self._transport.request_json("POST", "/projects", json=dict(body)))["data"]

    async def get(self, project: str) -> dict[str, Any]:
        return (await self._transport.request_json("GET", f"/projects/{enc(project)}"))["data"]

    async def update(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "PATCH",
                f"/projects/{enc(project)}",
                json=dict(body),
            )
        )["data"]

    async def delete(self, project: str) -> None:
        await self._transport.request_raw("DELETE", f"/projects/{enc(project)}")

    async def activate(self, project: str) -> dict[str, Any]:
        return (
            await self._transport.request_json("POST", f"/projects/{enc(project)}/activate")
        )["data"]

    async def deactivate(self, project: str) -> dict[str, Any]:
        return (
            await self._transport.request_json("POST", f"/projects/{enc(project)}/deactivate")
        )["data"]

    async def event_handlers(self, project: str) -> dict[str, Any]:
        return await self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/event-handlers",
        )

    async def schedules(self, project: str) -> dict[str, Any]:
        return await self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/schedules",
        )


class AsyncBuildsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/builds", params=query)

    async def list_for_project(self, project: str, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json(
            "GET",
            f"/projects/{enc(project)}/builds",
            params=query,
        )

    async def get(self, project: str, build_id: str) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "GET",
                f"/projects/{enc(project)}/builds/{enc(build_id)}",
            )
        )["data"]

    async def deployed(self, project: str) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "GET",
                f"/projects/{enc(project)}/builds/deployed",
            )
        )["data"]

    async def live(self, project: str) -> dict[str, Any]:
        return (
            await self._transport.request_json("GET", f"/projects/{enc(project)}/builds/live")
        )["data"]

    async def upload(
        self,
        project: str,
        *,
        file: Any,
        hash: str,
        build_id: str | None = None,
    ) -> dict[str, Any]:
        data = {"hash": hash, **({"build_id": build_id} if build_id else {})}
        response = await self._transport.request_raw(
            "POST",
            f"/projects/{enc(project)}/builds",
            data=data,
            files={"file": file},
            headers={"Accept": "application/json"},
        )
        return response.json()["data"]

    async def download(self, project: str, build_id: str):
        return await self._transport.request_raw(
            "GET",
            f"/projects/{enc(project)}/builds/{enc(build_id)}/download",
        )

    async def deploy(self, project: str, build_id: str) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "POST",
                f"/projects/{enc(project)}/builds/{enc(build_id)}/deploy",
            )
        )["data"]


class AsyncContextResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, project: str) -> dict[str, Any]:
        return await self._transport.request_json("GET", f"/projects/{enc(project)}/context")

    async def create(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "POST",
                f"/projects/{enc(project)}/context",
                json=dict(body),
            )
        )["data"]

    async def update(self, project: str, key: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "PUT",
                f"/projects/{enc(project)}/context/{enc(key)}",
                json=dict(body),
            )
        )["data"]

    async def delete(self, project: str, key: str) -> None:
        await self._transport.request_raw(
            "DELETE",
            f"/projects/{enc(project)}/context/{enc(key)}",
        )


class AsyncDomainsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (await self._transport.request_json("POST", "/domains", json=dict(body)))["data"]

    async def list(self) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/domains")

    async def get(self, domain_id: str) -> dict[str, Any]:
        return (await self._transport.request_json("GET", f"/domains/{enc(domain_id)}"))["data"]

    async def delete(self, domain_id: str) -> None:
        await self._transport.request_raw("DELETE", f"/domains/{enc(domain_id)}")

    async def verify(self, domain_id: str) -> dict[str, Any]:
        return (
            await self._transport.request_json("POST", f"/domains/{enc(domain_id)}/verify")
        )["data"]


class AsyncSessionsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (await self._transport.request_json("POST", "/sessions", json=dict(body)))["data"]

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/sessions", params=query)

    async def revoke(self, session_id: str) -> None:
        await self._transport.request_raw("DELETE", f"/sessions/{enc(session_id)}")

    async def revoke_all(self) -> dict[str, Any]:
        return (await self._transport.request_json("DELETE", "/sessions"))["data"]


class AsyncServiceKeysResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        return (
            await self._transport.request_json("POST", "/service-keys", json=dict(body))
        )["data"]

    async def list(self, **query: Any) -> dict[str, Any]:
        return await self._transport.request_json("GET", "/service-keys", params=query)

    async def get(self, service_key_id: str) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "GET",
                f"/service-keys/{enc(service_key_id)}",
            )
        )["data"]

    async def update(self, service_key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return (
            await self._transport.request_json(
                "PATCH",
                f"/service-keys/{enc(service_key_id)}",
                json=dict(body),
            )
        )["data"]

    async def revoke(self, service_key_id: str) -> None:
        await self._transport.request_raw("DELETE", f"/service-keys/{enc(service_key_id)}")

    async def revoke_all(self) -> dict[str, Any]:
        return (await self._transport.request_json("DELETE", "/service-keys"))["data"]


async def aiter_stream(
    transport: AsyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
) -> AsyncIterator[StreamEvent]:
    async with transport.client.stream(
        method,
        f"{transport.api_base_url}{path}",
        headers=transport.headers(
            {
                "Accept": "text/event-stream",
                **({"Content-Type": "application/json"} if json is not None else {}),
            }
        ),
        json=dict(json) if json is not None else None,
        params=without_none(params),
    ) as response:
        raise_for_status(response)
        buffer = ""
        async for chunk in response.aiter_text():
            buffer += chunk
            events, buffer = consume_sse_blocks(buffer)
            for event in events:
                yield event
        events, _ = consume_sse_blocks(f"{buffer}\n\n")
        for event in events:
            yield event
