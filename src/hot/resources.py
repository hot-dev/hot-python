from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable, Generator, Iterator, Mapping
from contextlib import aclosing, asynccontextmanager, closing, contextmanager
from typing import Any
from urllib.parse import quote

import httpx

from . import _streaming
from ._streaming import ReconnectPolicy, RunCorrelator, StreamWake, aiter_stream, iter_stream
from ._transport import AsyncTransport, SyncTransport, parse_data
from .errors import HotRunError, HotTaskError
from .types import StreamEvent


def enc(value: str) -> str:
    """Percent-encode a single path segment, including any slash."""
    if value in ("", ".", ".."):
        raise ValueError(f"Invalid path segment: {value!r}")
    return quote(value, safe="")


def enc_path(path: str) -> str:
    """Percent-encode each segment of a slash-separated path."""
    trimmed = path.removeprefix("/")
    if not trimmed:
        raise ValueError(f"Invalid path: {path!r}")
    parts = trimmed.split("/")
    for part in parts:
        if part in ("", ".", ".."):
            raise ValueError(f"Invalid path segment {part!r} in path {path!r}")
    return "/".join(quote(part, safe="") for part in parts)


def _extract_run_result(result: Any) -> Any:
    if isinstance(result, dict) and "$ok" in result:
        return result["$ok"]
    return result


def _snapshot_policy(kind: str, timeout: float, max_attempts: int) -> ReconnectPolicy:
    return ReconnectPolicy(
        timeout=timeout,
        max_attempts=max_attempts,
        timeout_message=f"Timeout waiting for {kind}",
        ended_message=f"{kind.capitalize()} subscription ended before the {kind} completed",
    )


def _terminal_run(event: StreamEvent) -> dict[str, Any] | None:
    """Return a succeeded run snapshot; raise HotRunError for a failed or cancelled one."""
    run = event.get("run") if event.get("type") == "run:update" else None
    if not isinstance(run, dict):
        return None
    status = run.get("status")
    if status == "succeeded":
        return run
    if status in ("failed", "cancelled"):
        raise HotRunError(run)
    return None


def _terminal_task(event: StreamEvent) -> dict[str, Any] | None:
    """Return a completed task snapshot; raise HotTaskError for any other final state."""
    task = event.get("task") if event.get("type") == "task:update" else None
    if not isinstance(task, dict):
        return None
    status = task.get("status")
    if status == "completed":
        return task
    if status in ("failed", "cancelled", "timed_out"):
        raise HotTaskError(task)
    return None


class EventsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def publish(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Publish an event and return the created event data."""
        return self._transport.request_data("POST", "/events", json=dict(body))

    def call_hot(
        self,
        fn: str,
        args: list[Any] | None = None,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
        max_attempts: int = 5,
    ) -> Any:
        """Call a Hot function and wait for its result, unwrapping ``{"$ok": ...}``.

        ``timeout`` bounds the whole call. ``max_attempts`` bounds consecutive
        failed connections; the API's 5-minute stream rollovers do not count.
        ``on_chunk`` receives ``stream:data`` text chunks of the call's run.
        """
        run = wait_for_call_result(
            self._transport,
            fn,
            args or [],
            timeout=timeout,
            on_chunk=on_chunk,
            max_attempts=max_attempts,
        )
        return _extract_run_result(run.get("result"))

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the event list envelope (data + pagination)."""
        return self._transport.request_json("GET", "/events", params=query)

    def get(self, event_id: str) -> dict[str, Any]:
        """Return a single event."""
        return self._transport.request_data("GET", f"/events/{enc(event_id)}")

    def get_runs(self, event_id: str) -> dict[str, Any]:
        """Return the runs triggered by an event."""
        return self._transport.request_json("GET", f"/events/{enc(event_id)}/runs")


class RunsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the run list envelope (data + pagination)."""
        return self._transport.request_json("GET", "/runs", params=query)

    def stats(self) -> dict[str, Any]:
        """Return aggregate run statistics."""
        return self._transport.request_data("GET", "/runs/stats")

    def get(self, run_id: str) -> dict[str, Any]:
        """Return a single run."""
        return self._transport.request_data("GET", f"/runs/{enc(run_id)}")

    def purge_calls(self, run_id: str) -> dict[str, Any]:
        """Delete the run's call traces."""
        return self._transport.request_data("DELETE", f"/runs/{enc(run_id)}/calls")

    def subscribe(self, run_id: str) -> Generator[StreamEvent, None, None]:
        """Yield durable run snapshots until the run becomes terminal."""
        yield from iter_stream(
            self._transport,
            "GET",
            f"/runs/{enc(run_id)}/subscribe",
        )

    def wait(
        self,
        run_id: str,
        *,
        timeout: float = 60.0,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for run success; failed/cancelled runs raise HotRunError.

        ``timeout`` bounds the whole wait. ``max_attempts`` bounds consecutive
        failed connections; see :class:`hot._streaming.ReconnectPolicy`.
        """
        policy = _snapshot_policy("run", timeout, max_attempts)
        while True:
            policy.connect()
            try:
                with closing(
                    iter_stream(
                        self._transport,
                        "GET",
                        f"/runs/{enc(run_id)}/subscribe",
                        deadline=policy.deadline,
                    )
                ) as events:
                    for event in events:
                        policy.saw(event)
                        run = _terminal_run(event)
                        if run is not None:
                            return run
            except Exception as error:
                delay = policy.after_error(error)
                if delay is None:
                    raise
            else:
                delay = policy.after_end()
            _streaming.sleep(delay)


class EnvResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self) -> dict[str, Any]:
        """Return environment info."""
        return self._transport.request_data("GET", "/env")

    def subscribe(self) -> Generator[StreamEvent, None, None]:
        """Stream environment events (requires API key credentials and pub/sub)."""
        yield from iter_stream(self._transport, "GET", "/env/subscribe")

    def purge_calls(self) -> dict[str, Any]:
        """Delete every call trace in the environment."""
        return self._transport.request_data("DELETE", "/env/calls")


class OrgResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def usage(self) -> dict[str, Any]:
        """Return organization usage and limits."""
        return self._transport.request_data("GET", "/org/usage")


class StreamsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def subscribe(
        self, stream_id: str, *, project: str | None = None
    ) -> Generator[StreamEvent, None, None]:
        """Stream events from an existing run stream."""
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
    ) -> Generator[StreamEvent, None, None]:
        """Stream events from an existing run stream via POST."""
        yield from iter_stream(
            self._transport,
            "POST",
            f"/streams/{enc(stream_id)}/subscribe",
            params={"project": project},
        )

    def purge_calls(self, stream_id: str) -> dict[str, Any]:
        """Delete the call traces of every run on the stream."""
        return self._transport.request_data("DELETE", f"/streams/{enc(stream_id)}/calls")

    def wait_for_run_result(
        self,
        stream_id: str,
        event_id: str,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for the run created by ``event_id``; run:fail/run:cancel raise HotRunError.

        A run with status ``pending_retry`` is not final: the wait follows the
        retry run, linked by ``origin_run_id``. A ``failed`` run whose retry
        was scheduled is raised only if no retry run appears within 5 seconds
        (or by ``timeout``). ``timeout`` bounds the whole wait; ``max_attempts``
        bounds consecutive failed connections.
        """
        return wait_for_run_result(
            self._transport,
            stream_id,
            event_id,
            timeout=timeout,
            on_chunk=on_chunk,
            max_attempts=max_attempts,
        )

    def subscribe_with_event(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        reconnect: bool = True,
        max_attempts: int = 5,
        timeout: float | None = None,
    ) -> Generator[StreamEvent, None, None]:
        """Atomically subscribe then publish.

        The Hot API closes every stream subscription 5 minutes after it opens,
        with ``stream:complete``. When ``reconnect=True`` (the default), the SDK
        resubscribes to the same ``stream_id`` and keeps yielding events until
        the published event's run ends: ``run:stop``, ``run:cancel``, or a
        final ``run:fail``. A run with status ``pending_retry`` is not final (a
        handler retry continues on a new run linked by ``origin_run_id``), and
        a ``failed`` run whose retry was scheduled ends the iterator only if no
        retry run appears within 5 seconds. Replayed ``run:start`` and
        terminal events are deduped by ``run_id``. ``stream:data`` chunks are
        not replayed and may be lost across the reconnect window.

        Reconnects follow :class:`hot._streaming.ReconnectPolicy`: permanent
        4xx errors raise at once, transient errors back off, and
        ``max_attempts`` counts consecutive failed connections. ``timeout``
        (seconds) optionally bounds the whole subscription and raises
        ``TimeoutError``.

        Pass ``reconnect=False`` for the 1.0.x single-connection behavior.
        """
        if not reconnect:
            policy = _event_policy(timeout, max_attempts)
            yield from self._subscribe_with_event_once(
                body, project=project, deadline=policy.deadline
            )
            return

        policy = _event_policy(timeout, max_attempts)
        runs = RunCorrelator(None)
        dedupe = _RunEventDedupe()
        stream_id: str | None = None
        try:
            while True:
                policy.connect()
                if stream_id is None:
                    source = self._subscribe_with_event_once(
                        body, project=project, deadline=policy.deadline, wake=runs.wake_at
                    )
                else:
                    source = iter_stream(
                        self._transport,
                        "GET",
                        f"/streams/{enc(stream_id)}/subscribe",
                        params={"project": project},
                        deadline=policy.deadline,
                        wake=runs.wake_at,
                    )
                try:
                    with closing(source) as events:
                        for event in events:
                            policy.saw(event)
                            published_stream, published_event = _published_ids(event)
                            if published_stream and published_event:
                                stream_id, runs.event_id = published_stream, published_event
                            if dedupe.replayed(event):
                                continue
                            ended = runs.observe(event)
                            yield event
                            if ended:
                                return
                except StreamWake:
                    # A failure held for its retry stands; its run:fail was yielded.
                    return
                except Exception as error:
                    try:
                        delay = policy.after_error(
                            error, published=stream_id is not None, wake_at=runs.wake_at()
                        )
                    except StreamWake:
                        return
                    if delay is None:
                        raise
                else:
                    if stream_id is None:
                        raise RuntimeError("stream ended before event:published was received")
                    try:
                        delay = policy.after_end(wake_at=runs.wake_at())
                    except StreamWake:
                        return
                _streaming.sleep(delay)
        except TimeoutError:
            # A failure held for its retry stands at the deadline; its
            # run:fail was yielded.
            if runs.wake_at() is None:
                raise

    def _subscribe_with_event_once(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        deadline: float | None = None,
        wake: _streaming.Wake | None = None,
    ) -> Generator[StreamEvent, None, None]:
        yield from iter_stream(
            self._transport,
            "POST",
            "/streams/subscribe-with-event",
            json=dict(body),
            params={"project": project},
            deadline=deadline,
            wake=wake,
        )


class TasksResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self, task_id: str) -> dict[str, Any]:
        """Return the latest durable task snapshot."""
        return self._transport.request_data("GET", f"/tasks/{enc(task_id)}")

    def subscribe(self, task_id: str) -> Generator[StreamEvent, None, None]:
        """Yield durable task snapshots until the task becomes terminal."""
        yield from iter_stream(
            self._transport,
            "GET",
            f"/tasks/{enc(task_id)}/subscribe",
        )

    def wait(
        self,
        task_id: str,
        *,
        timeout: float = 60.0,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for task completion; failed/cancelled/timed-out tasks raise HotTaskError.

        ``timeout`` bounds the whole wait. ``max_attempts`` bounds consecutive
        failed connections; see :class:`hot._streaming.ReconnectPolicy`.
        """
        policy = _snapshot_policy("task", timeout, max_attempts)
        while True:
            policy.connect()
            try:
                with closing(
                    iter_stream(
                        self._transport,
                        "GET",
                        f"/tasks/{enc(task_id)}/subscribe",
                        deadline=policy.deadline,
                    )
                ) as events:
                    for event in events:
                        policy.saw(event)
                        task = _terminal_task(event)
                        if task is not None:
                            return task
            except Exception as error:
                delay = policy.after_error(error)
                if delay is None:
                    raise
            else:
                delay = policy.after_end()
            _streaming.sleep(delay)


class FilesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the file list envelope (data + pagination)."""
        return self._transport.request_json("GET", "/files", params=query)

    def get(self, file_id: str) -> dict[str, Any]:
        """Return a single file's metadata."""
        return self._transport.request_data("GET", f"/files/{enc(file_id)}")

    def delete(self, file_id: str) -> None:
        """Delete a file."""
        self._transport.request_json("DELETE", f"/files/{enc(file_id)}")

    def download(self, file_id: str) -> httpx.Response:
        """Return the HTTP response with the whole file read into memory.

        Use :meth:`download_stream` for large files.
        """
        return self._transport.request_raw("GET", f"/files/{enc(file_id)}/download")

    @contextmanager
    def download_stream(self, file_id: str) -> Iterator[httpx.Response]:
        """Stream the file content without buffering it.

        ``with hot.files.download_stream(file_id) as response:`` then iterate
        ``response.iter_bytes()``. The connection closes when the block exits.
        """
        with self._transport.stream_raw("GET", f"/files/{enc(file_id)}/download") as response:
            yield response

    def upload(
        self,
        path: str,
        content: bytes | str,
        *,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload content to a slash-separated file path."""
        response = self._transport.request_raw(
            "PUT",
            f"/files/upload/{enc_path(path)}",
            content=content,
            headers={
                "Accept": "application/json",
                **({"Content-Type": content_type} if content_type else {}),
            },
        )
        return parse_data(response)

    def initiate_upload(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Start a multipart upload."""
        return self._transport.request_data("POST", "/files/uploads", json=dict(body))

    def upload_part(self, upload_id: str, part_number: int, content: bytes | str) -> dict[str, Any]:
        """Upload one part of a multipart upload."""
        response = self._transport.request_raw(
            "PUT",
            f"/files/uploads/{enc(upload_id)}/{part_number}",
            content=content,
            headers={"Accept": "application/json"},
        )
        return parse_data(response)

    def complete_upload(self, upload_id: str) -> dict[str, Any]:
        """Finish a multipart upload."""
        return self._transport.request_data(
            "POST",
            f"/files/uploads/{enc(upload_id)}/complete",
        )

    def abort_upload(self, upload_id: str) -> None:
        """Cancel a multipart upload."""
        self._transport.request_json("DELETE", f"/files/uploads/{enc(upload_id)}")


class BlobsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def download(self, blob_ref_id: str) -> httpx.Response:
        """Return the HTTP response with the whole blob read into memory.

        A large payload spilled to blob storage, in the caller's environment.
        Needs an environment API key; string payloads are served as UTF-8
        text. Use :meth:`download_stream` for large blobs.
        """
        return self._transport.request_raw("GET", f"/blobs/{enc(blob_ref_id)}/download")

    @contextmanager
    def download_stream(self, blob_ref_id: str) -> Iterator[httpx.Response]:
        """Stream the blob content without buffering it.

        ``with hot.blobs.download_stream(blob_ref_id) as response:`` then
        iterate ``response.iter_bytes()``. The connection closes when the block
        exits.
        """
        with self._transport.stream_raw("GET", f"/blobs/{enc(blob_ref_id)}/download") as response:
            yield response


class ProjectsResource:
    def __init__(self, transport: SyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the project list envelope (data + pagination)."""
        return self._transport.request_json("GET", f"{self._prefix}/projects", params=query)

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a project."""
        return self._transport.request_data("POST", f"{self._prefix}/projects", json=dict(body))

    def get(self, project: str) -> dict[str, Any]:
        """Return a project by id or slug."""
        return self._transport.request_data("GET", f"{self._prefix}/projects/{enc(project)}")

    def update(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Patch a project."""
        return self._transport.request_data(
            "PATCH",
            f"{self._prefix}/projects/{enc(project)}",
            json=dict(body),
        )

    def delete(self, project: str) -> None:
        """Delete a project."""
        self._transport.request_json("DELETE", f"{self._prefix}/projects/{enc(project)}")

    def activate(self, project: str) -> dict[str, Any]:
        """Activate a project."""
        return self._transport.request_data(
            "POST", f"{self._prefix}/projects/{enc(project)}/activate"
        )

    def deactivate(self, project: str) -> dict[str, Any]:
        """Deactivate a project."""
        return self._transport.request_data(
            "POST", f"{self._prefix}/projects/{enc(project)}/deactivate"
        )

    def event_handlers(self, project: str) -> dict[str, Any]:
        """Return the project's event handlers."""
        return self._transport.request_json(
            "GET", f"{self._prefix}/projects/{enc(project)}/event-handlers"
        )

    def schedules(self, project: str) -> dict[str, Any]:
        """Return the project's schedules."""
        return self._transport.request_json(
            "GET", f"{self._prefix}/projects/{enc(project)}/schedules"
        )


class BuildsResource:
    def __init__(self, transport: SyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the build list envelope across all projects."""
        return self._transport.request_json("GET", f"{self._prefix}/builds", params=query)

    def list_for_project(self, project: str, **query: Any) -> dict[str, Any]:
        """Return the build list envelope for one project."""
        return self._transport.request_json(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds",
            params=query,
        )

    def get(self, project: str, build_id: str) -> dict[str, Any]:
        """Return a single build."""
        return self._transport.request_data(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}",
        )

    def deployed(self, project: str) -> dict[str, Any]:
        """Return the project's deployed build."""
        return self._transport.request_data(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/deployed",
        )

    def live(self, project: str) -> dict[str, Any]:
        """Return the project's live build."""
        return self._transport.request_data(
            "GET", f"{self._prefix}/projects/{enc(project)}/builds/live"
        )

    def upload(
        self,
        project: str,
        *,
        file: Any,
        hash: str,
        build_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload a build archive as multipart form data."""
        data = {"hash": hash, **({"build_id": build_id} if build_id else {})}
        response = self._transport.request_raw(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/builds",
            data=data,
            files={"file": file},
            headers={"Accept": "application/json"},
        )
        return parse_data(response)

    def download(self, project: str, build_id: str) -> httpx.Response:
        """Return the HTTP response with the whole build archive read into memory.

        Use :meth:`download_stream` for large archives.
        """
        return self._transport.request_raw(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/download",
        )

    @contextmanager
    def download_stream(self, project: str, build_id: str) -> Iterator[httpx.Response]:
        """Stream the build archive without buffering it; iterate ``response.iter_bytes()``."""
        with self._transport.stream_raw(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/download",
        ) as response:
            yield response

    def deploy(self, project: str, build_id: str) -> dict[str, Any]:
        """Deploy a build."""
        return self._transport.request_data(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/deploy",
        )


class ContextResource:
    def __init__(self, transport: SyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    def list(self, project: str) -> dict[str, Any]:
        """Return the context variable list envelope for a project."""
        return self._transport.request_json(
            "GET", f"{self._prefix}/projects/{enc(project)}/context"
        )

    def create(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a context variable."""
        return self._transport.request_data(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/context",
            json=dict(body),
        )

    def update(self, project: str, key: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Replace a context variable."""
        return self._transport.request_data(
            "PUT",
            f"{self._prefix}/projects/{enc(project)}/context/{enc(key)}",
            json=dict(body),
        )

    def delete(self, project: str, key: str) -> None:
        """Delete a context variable."""
        self._transport.request_json(
            "DELETE", f"{self._prefix}/projects/{enc(project)}/context/{enc(key)}"
        )


class DomainsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Register a custom domain."""
        return self._transport.request_data("POST", "/domains", json=dict(body))

    def list(self) -> dict[str, Any]:
        """Return the domain list envelope."""
        return self._transport.request_json("GET", "/domains")

    def get(self, domain_id: str) -> dict[str, Any]:
        """Return a single domain."""
        return self._transport.request_data("GET", f"/domains/{enc(domain_id)}")

    def delete(self, domain_id: str) -> None:
        """Delete a domain."""
        self._transport.request_json("DELETE", f"/domains/{enc(domain_id)}")

    def verify(self, domain_id: str) -> dict[str, Any]:
        """Trigger domain verification."""
        return self._transport.request_data("POST", f"/domains/{enc(domain_id)}/verify")


class SessionsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a scoped session."""
        return self._transport.request_data("POST", "/sessions", json=dict(body))

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the session list envelope."""
        return self._transport.request_json("GET", "/sessions", params=query)

    def revoke(self, session_id: str) -> None:
        """Revoke one session."""
        self._transport.request_json("DELETE", f"/sessions/{enc(session_id)}")

    def revoke_all(self) -> dict[str, Any]:
        """Revoke every session."""
        return self._transport.request_data("DELETE", "/sessions")


class ServiceKeysResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a scoped service key."""
        return self._transport.request_data("POST", "/service-keys", json=dict(body))

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the service key list envelope."""
        return self._transport.request_json("GET", "/service-keys", params=query)

    def get(self, service_key_id: str) -> dict[str, Any]:
        """Return a single service key."""
        return self._transport.request_data(
            "GET",
            f"/service-keys/{enc(service_key_id)}",
        )

    def update(self, service_key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Patch a service key."""
        return self._transport.request_data(
            "PATCH",
            f"/service-keys/{enc(service_key_id)}",
            json=dict(body),
        )

    def revoke(self, service_key_id: str) -> None:
        """Revoke one service key."""
        self._transport.request_json("DELETE", f"/service-keys/{enc(service_key_id)}")

    def revoke_all(self) -> dict[str, Any]:
        """Revoke every service key."""
        return self._transport.request_data("DELETE", "/service-keys")


class OrgEnv:
    """Projects, builds and context in one environment of an org, through an org API key.

    Paths are ``/v1/orgs/{org_id}/envs/{env_id}/...``; the org key's grants for
    that environment authorize each call as an environment key would.
    """

    def __init__(self, transport: SyncTransport, org_id: str, env_id: str) -> None:
        prefix = f"/orgs/{enc(org_id)}/envs/{enc(env_id)}"
        self.org_id = org_id
        self.env_id = env_id
        self.projects = ProjectsResource(transport, prefix)
        self.builds = BuildsResource(transport, prefix)
        self.context = ContextResource(transport, prefix)


class OrgsResource:
    """Organizations, their environments and API keys. Requires an org API key."""

    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def list(self, **query: Any) -> dict[str, Any]:
        """Return the list envelope of sub-organizations the key may read."""
        return self._transport.request_json("GET", "/orgs", params=query)

    def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a sub-organization with its default environment.

        ``body`` takes ``name``, ``slug`` (32 characters or fewer),
        ``idempotency_key`` and optional ``external_id``. Returns ``org``,
        ``env_id`` and ``created`` (false when the idempotency key matched an
        earlier request).
        """
        return self._transport.request_data("POST", "/orgs", json=dict(body))

    def get(self, org_id: str) -> dict[str, Any]:
        """Return one organization."""
        return self._transport.request_data("GET", f"/orgs/{enc(org_id)}")

    def update(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Set a sub-org's ``state``: ``active``, ``suspended`` or ``archived``. Returns the org."""
        return self._transport.request_data("PATCH", f"/orgs/{enc(org_id)}", json=dict(body))

    def usage(self, org_id: str) -> dict[str, Any]:
        """Return usage across the parent organization and its sub-orgs; pass the parent's ID."""
        return self._transport.request_data("GET", f"/orgs/{enc(org_id)}/usage")

    def purge_calls(self, org_id: str, *, include_sub_orgs: bool = False) -> dict[str, Any]:
        """Delete every call trace in the org's environments (and its sub-organizations')."""
        return self._transport.request_data(
            "DELETE",
            f"/orgs/{enc(org_id)}/calls",
            params={"include_sub_orgs": "true" if include_sub_orgs else None},
        )

    def list_envs(self, org_id: str) -> dict[str, Any]:
        """Return the environment list envelope for an org."""
        return self._transport.request_json("GET", f"/orgs/{enc(org_id)}/envs")

    def create_env(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create an environment; idempotent by exact ``name`` within the org."""
        return self._transport.request_data("POST", f"/orgs/{enc(org_id)}/envs", json=dict(body))

    def get_env(self, org_id: str, env_id: str) -> dict[str, Any]:
        """Return one environment."""
        return self._transport.request_data("GET", f"/orgs/{enc(org_id)}/envs/{enc(env_id)}")

    def list_api_keys(self, org_id: str, **query: Any) -> dict[str, Any]:
        """Return the list envelope of the org's own org API keys."""
        return self._transport.request_json("GET", f"/orgs/{enc(org_id)}/api-keys", params=query)

    def create_api_key(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Mint an org API key (``description``, ``permissions``, optional ``expires_at``).

        The response's ``token`` is returned only once.
        """
        return self._transport.request_data(
            "POST", f"/orgs/{enc(org_id)}/api-keys", json=dict(body)
        )

    def list_env_api_keys(self, org_id: str, env_id: str, **query: Any) -> dict[str, Any]:
        """Return the list envelope of an environment's API keys."""
        return self._transport.request_json(
            "GET", f"/orgs/{enc(org_id)}/envs/{enc(env_id)}/api-keys", params=query
        )

    def create_env_api_key(
        self, org_id: str, env_id: str, body: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Mint an environment API key within the org key's grants for that environment.

        The response's ``token`` is returned only once.
        """
        return self._transport.request_data(
            "POST",
            f"/orgs/{enc(org_id)}/envs/{enc(env_id)}/api-keys",
            json=dict(body),
        )

    def env(self, org_id: str, env_id: str) -> OrgEnv:
        """Projects, builds and context in one of the org's environments."""
        return OrgEnv(self._transport, org_id, env_id)


class ApiKeysResource:
    """Org and environment API keys by ID. Requires an org API key that issued them."""

    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self, key_id: str) -> dict[str, Any]:
        """Return one API key (never its secret)."""
        return self._transport.request_data("GET", f"/api-keys/{enc(key_id)}")

    def update(self, key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Replace a key's ``permissions``; they must fit inside the caller's own."""
        return self._transport.request_data("PATCH", f"/api-keys/{enc(key_id)}", json=dict(body))

    def revoke(self, key_id: str) -> None:
        """Revoke a key. Everything it issued stops on its next request."""
        self._transport.request_json("DELETE", f"/api-keys/{enc(key_id)}")

    def rotate(self, key_id: str, *, overlap_seconds: int | None = None) -> dict[str, Any]:
        """Issue a new secret for the same key; the old one works for ``overlap_seconds``.

        The server's default overlap is 86400 seconds (maximum 604800). The
        response's ``token`` is returned only once.
        """
        body = {} if overlap_seconds is None else {"overlap_seconds": overlap_seconds}
        return self._transport.request_data("POST", f"/api-keys/{enc(key_id)}/rotate", json=body)


def _event_policy(timeout: float | None, max_attempts: int) -> ReconnectPolicy:
    return ReconnectPolicy(
        timeout=timeout,
        max_attempts=max_attempts,
        timeout_message="Timeout waiting for the published event's run",
        ended_message="stream ended before the published event's run completed",
    )


_MAX_DEDUPE_ENTRIES = 10_000


class _RunEventDedupe:
    """Drop ``run:start`` and terminal events a reconnect replays."""

    def __init__(self, max_entries: int = _MAX_DEDUPE_ENTRIES) -> None:
        self._max_entries = max_entries
        self._seen: dict[tuple[str, str], None] = {}

    def replayed(self, event: StreamEvent) -> bool:
        event_type = event.get("type")
        if event_type not in ("run:start", "run:stop", "run:fail", "run:cancel"):
            return False
        run = event.get("run")
        if not isinstance(run, dict):
            return False
        run_id = run.get("run_id")
        if not isinstance(run_id, str):
            return False
        if event_type == "run:start":
            kind = "start"
        elif run.get("status") == "pending_retry":
            kind = "pending_retry"
        else:
            kind = "end"
        key = (kind, run_id)
        if key in self._seen:
            return True
        if len(self._seen) >= self._max_entries and self._seen:
            self._seen.pop(next(iter(self._seen)))
        if self._max_entries > 0:
            self._seen[key] = None
        return False


def _run_policy(timeout: float | None, max_attempts: int) -> ReconnectPolicy:
    return ReconnectPolicy(
        timeout=timeout,
        max_attempts=max_attempts,
        timeout_message="Timeout waiting for run result",
        ended_message="Stream ended before run completed",
    )


def _call_body(fn: str, args: list[Any]) -> dict[str, Any]:
    return {"event_type": "hot:call", "event_data": {"fn": fn, "args": args}}


def _published_ids(event: StreamEvent) -> tuple[str | None, str | None]:
    if event.get("type") != "event:published":
        return None, None
    stream_id, event_id = event.get("stream_id"), event.get("event_id")
    return (
        stream_id if isinstance(stream_id, str) else None,
        event_id if isinstance(event_id, str) else None,
    )


def wait_for_run_result(
    transport: SyncTransport,
    stream_id: str,
    event_id: str,
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
    max_attempts: int = 5,
) -> dict[str, Any]:
    """Wait for the run created by ``event_id``; run:fail/run:cancel raise HotRunError."""
    policy = _run_policy(timeout, max_attempts)
    runs = RunCorrelator(event_id, on_chunk)
    try:
        while True:
            policy.connect()
            try:
                with closing(
                    iter_stream(
                        transport,
                        "GET",
                        f"/streams/{enc(stream_id)}/subscribe",
                        deadline=policy.deadline,
                        wake=runs.wake_at,
                    )
                ) as events:
                    for event in events:
                        policy.saw(event)
                        run = runs.handle(event)
                        if run is not None:
                            return run
            except StreamWake:
                runs.expire()
                delay = 0.0
            except Exception as error:
                try:
                    retry_delay = policy.after_error(error, wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
                else:
                    if retry_delay is None:
                        raise
                    delay = retry_delay
            else:
                try:
                    delay = policy.after_end(wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
            _streaming.sleep(delay)
    except TimeoutError:
        # A failure held for its retry stands at the deadline.
        runs.expire()
        raise


def wait_for_call_result(
    transport: SyncTransport,
    fn: str,
    args: list[Any],
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
    max_attempts: int = 5,
) -> dict[str, Any]:
    """Atomically publish hot:call and wait for its correlated run."""
    policy = _run_policy(timeout, max_attempts)
    runs = RunCorrelator(None, on_chunk)
    stream_id: str | None = None
    try:
        while True:
            policy.connect()
            if stream_id is None:
                source = iter_stream(
                    transport,
                    "POST",
                    "/streams/subscribe-with-event",
                    json=_call_body(fn, args),
                    deadline=policy.deadline,
                    wake=runs.wake_at,
                )
            else:
                source = iter_stream(
                    transport,
                    "GET",
                    f"/streams/{enc(stream_id)}/subscribe",
                    deadline=policy.deadline,
                    wake=runs.wake_at,
                )
            try:
                with closing(source) as events:
                    for event in events:
                        policy.saw(event)
                        published_stream, published_event = _published_ids(event)
                        if published_stream and published_event:
                            stream_id, runs.event_id = published_stream, published_event
                        run = runs.handle(event)
                        if run is not None:
                            return run
            except StreamWake:
                runs.expire()
                delay = 0.0
            except Exception as error:
                try:
                    retry_delay = policy.after_error(
                        error, published=stream_id is not None, wake_at=runs.wake_at()
                    )
                except StreamWake:
                    runs.expire()
                    delay = 0.0
                else:
                    if retry_delay is None:
                        raise
                    delay = retry_delay
            else:
                if stream_id is None:
                    raise RuntimeError("stream ended before event:published was received")
                try:
                    delay = policy.after_end(wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
            _streaming.sleep(delay)
    except TimeoutError:
        # A failure held for its retry stands at the deadline.
        runs.expire()
        raise


async def async_wait_for_run_result(
    transport: AsyncTransport,
    stream_id: str,
    event_id: str,
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
    max_attempts: int = 5,
) -> dict[str, Any]:
    """Wait for the run created by ``event_id``; run:fail/run:cancel raise HotRunError."""
    policy = _run_policy(timeout, max_attempts)
    runs = RunCorrelator(event_id, on_chunk)
    try:
        while True:
            policy.connect()
            try:
                async with aclosing(
                    aiter_stream(
                        transport,
                        "GET",
                        f"/streams/{enc(stream_id)}/subscribe",
                        deadline=policy.deadline,
                        wake=runs.wake_at,
                    )
                ) as events:
                    async for event in events:
                        policy.saw(event)
                        run = runs.handle(event)
                        if run is not None:
                            return run
            except StreamWake:
                runs.expire()
                delay = 0.0
            except Exception as error:
                try:
                    retry_delay = policy.after_error(error, wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
                else:
                    if retry_delay is None:
                        raise
                    delay = retry_delay
            else:
                try:
                    delay = policy.after_end(wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
            await _streaming.asleep(delay)
    except TimeoutError:
        # A failure held for its retry stands at the deadline.
        runs.expire()
        raise


async def async_wait_for_call_result(
    transport: AsyncTransport,
    fn: str,
    args: list[Any],
    *,
    timeout: float = 60.0,
    on_chunk: Callable[[str], None] | None = None,
    max_attempts: int = 5,
) -> dict[str, Any]:
    """Async atomic hot:call publish and correlated run wait."""
    policy = _run_policy(timeout, max_attempts)
    runs = RunCorrelator(None, on_chunk)
    stream_id: str | None = None
    try:
        while True:
            policy.connect()
            if stream_id is None:
                source = aiter_stream(
                    transport,
                    "POST",
                    "/streams/subscribe-with-event",
                    json=_call_body(fn, args),
                    deadline=policy.deadline,
                    wake=runs.wake_at,
                )
            else:
                source = aiter_stream(
                    transport,
                    "GET",
                    f"/streams/{enc(stream_id)}/subscribe",
                    deadline=policy.deadline,
                    wake=runs.wake_at,
                )
            try:
                async with aclosing(source) as events:
                    async for event in events:
                        policy.saw(event)
                        published_stream, published_event = _published_ids(event)
                        if published_stream and published_event:
                            stream_id, runs.event_id = published_stream, published_event
                        run = runs.handle(event)
                        if run is not None:
                            return run
            except StreamWake:
                runs.expire()
                delay = 0.0
            except Exception as error:
                try:
                    retry_delay = policy.after_error(
                        error, published=stream_id is not None, wake_at=runs.wake_at()
                    )
                except StreamWake:
                    runs.expire()
                    delay = 0.0
                else:
                    if retry_delay is None:
                        raise
                    delay = retry_delay
            else:
                if stream_id is None:
                    raise RuntimeError("stream ended before event:published was received")
                try:
                    delay = policy.after_end(wake_at=runs.wake_at())
                except StreamWake:
                    runs.expire()
                    delay = 0.0
            await _streaming.asleep(delay)
    except TimeoutError:
        # A failure held for its retry stands at the deadline.
        runs.expire()
        raise


class AsyncEventsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def publish(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Publish an event and return the created event data."""
        return await self._transport.request_data("POST", "/events", json=dict(body))

    async def call_hot(
        self,
        fn: str,
        args: list[Any] | None = None,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
        max_attempts: int = 5,
    ) -> Any:
        """Call a Hot function and wait for its result, unwrapping ``{"$ok": ...}``.

        ``timeout`` bounds the whole call. ``max_attempts`` bounds consecutive
        failed connections; the API's 5-minute stream rollovers do not count.
        ``on_chunk`` receives ``stream:data`` text chunks of the call's run.
        """
        run = await async_wait_for_call_result(
            self._transport,
            fn,
            args or [],
            timeout=timeout,
            on_chunk=on_chunk,
            max_attempts=max_attempts,
        )
        return _extract_run_result(run.get("result"))

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the event list envelope (data + pagination)."""
        return await self._transport.request_json("GET", "/events", params=query)

    async def get(self, event_id: str) -> dict[str, Any]:
        """Return a single event."""
        return await self._transport.request_data("GET", f"/events/{enc(event_id)}")

    async def get_runs(self, event_id: str) -> dict[str, Any]:
        """Return the runs triggered by an event."""
        return await self._transport.request_json("GET", f"/events/{enc(event_id)}/runs")


class AsyncRunsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the run list envelope (data + pagination)."""
        return await self._transport.request_json("GET", "/runs", params=query)

    async def stats(self) -> dict[str, Any]:
        """Return aggregate run statistics."""
        return await self._transport.request_data("GET", "/runs/stats")

    async def get(self, run_id: str) -> dict[str, Any]:
        """Return a single run."""
        return await self._transport.request_data("GET", f"/runs/{enc(run_id)}")

    async def purge_calls(self, run_id: str) -> dict[str, Any]:
        """Delete the run's call traces."""
        return await self._transport.request_data("DELETE", f"/runs/{enc(run_id)}/calls")

    async def subscribe(self, run_id: str) -> AsyncGenerator[StreamEvent, None]:
        """Yield durable run snapshots until the run becomes terminal."""
        async with aclosing(
            aiter_stream(
                self._transport,
                "GET",
                f"/runs/{enc(run_id)}/subscribe",
            )
        ) as events:
            async for event in events:
                yield event

    async def wait(
        self,
        run_id: str,
        *,
        timeout: float = 60.0,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for run success; failed/cancelled runs raise HotRunError."""
        policy = _snapshot_policy("run", timeout, max_attempts)
        while True:
            policy.connect()
            try:
                async with aclosing(
                    aiter_stream(
                        self._transport,
                        "GET",
                        f"/runs/{enc(run_id)}/subscribe",
                        deadline=policy.deadline,
                    )
                ) as events:
                    async for event in events:
                        policy.saw(event)
                        run = _terminal_run(event)
                        if run is not None:
                            return run
            except Exception as error:
                delay = policy.after_error(error)
                if delay is None:
                    raise
            else:
                delay = policy.after_end()
            await _streaming.asleep(delay)


class AsyncEnvResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self) -> dict[str, Any]:
        """Return environment info."""
        return await self._transport.request_data("GET", "/env")

    async def subscribe(self) -> AsyncGenerator[StreamEvent, None]:
        """Stream environment events (requires API key credentials and pub/sub)."""
        async with aclosing(aiter_stream(self._transport, "GET", "/env/subscribe")) as events:
            async for event in events:
                yield event

    async def purge_calls(self) -> dict[str, Any]:
        """Delete every call trace in the environment."""
        return await self._transport.request_data("DELETE", "/env/calls")


class AsyncOrgResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def usage(self) -> dict[str, Any]:
        """Return organization usage and limits."""
        return await self._transport.request_data("GET", "/org/usage")


class AsyncStreamsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def subscribe(
        self,
        stream_id: str,
        *,
        project: str | None = None,
    ) -> AsyncGenerator[StreamEvent, None]:
        """Stream events from an existing run stream."""
        async with aclosing(
            aiter_stream(
                self._transport,
                "GET",
                f"/streams/{enc(stream_id)}/subscribe",
                params={"project": project},
            )
        ) as events:
            async for event in events:
                yield event

    async def subscribe_post(
        self,
        stream_id: str,
        *,
        project: str | None = None,
    ) -> AsyncGenerator[StreamEvent, None]:
        """Stream events from an existing run stream via POST."""
        async with aclosing(
            aiter_stream(
                self._transport,
                "POST",
                f"/streams/{enc(stream_id)}/subscribe",
                params={"project": project},
            )
        ) as events:
            async for event in events:
                yield event

    async def purge_calls(self, stream_id: str) -> dict[str, Any]:
        """Delete the call traces of every run on the stream."""
        return await self._transport.request_data("DELETE", f"/streams/{enc(stream_id)}/calls")

    async def wait_for_run_result(
        self,
        stream_id: str,
        event_id: str,
        *,
        timeout: float = 60.0,
        on_chunk: Callable[[str], None] | None = None,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for the run created by ``event_id``; run:fail/run:cancel raise HotRunError.

        A run with status ``pending_retry`` is not final: the wait follows the
        retry run, linked by ``origin_run_id``. A ``failed`` run whose retry
        was scheduled is raised only if no retry run appears within 5 seconds
        (or by ``timeout``). ``timeout`` bounds the whole wait; ``max_attempts``
        bounds consecutive failed connections.
        """
        return await async_wait_for_run_result(
            self._transport,
            stream_id,
            event_id,
            timeout=timeout,
            on_chunk=on_chunk,
            max_attempts=max_attempts,
        )

    async def subscribe_with_event(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        reconnect: bool = True,
        max_attempts: int = 5,
        timeout: float | None = None,
    ) -> AsyncGenerator[StreamEvent, None]:
        """Async variant of :meth:`StreamsResource.subscribe_with_event`."""
        policy = _event_policy(timeout, max_attempts)
        if not reconnect:
            async with aclosing(
                self._subscribe_with_event_once(body, project=project, deadline=policy.deadline)
            ) as events:
                async for event in events:
                    yield event
            return

        runs = RunCorrelator(None)
        dedupe = _RunEventDedupe()
        stream_id: str | None = None
        try:
            while True:
                policy.connect()
                if stream_id is None:
                    source = self._subscribe_with_event_once(
                        body, project=project, deadline=policy.deadline, wake=runs.wake_at
                    )
                else:
                    source = aiter_stream(
                        self._transport,
                        "GET",
                        f"/streams/{enc(stream_id)}/subscribe",
                        params={"project": project},
                        deadline=policy.deadline,
                        wake=runs.wake_at,
                    )
                try:
                    async with aclosing(source) as events:
                        async for event in events:
                            policy.saw(event)
                            published_stream, published_event = _published_ids(event)
                            if published_stream and published_event:
                                stream_id, runs.event_id = published_stream, published_event
                            if dedupe.replayed(event):
                                continue
                            ended = runs.observe(event)
                            yield event
                            if ended:
                                return
                except StreamWake:
                    # A failure held for its retry stands; its run:fail was yielded.
                    return
                except Exception as error:
                    try:
                        delay = policy.after_error(
                            error, published=stream_id is not None, wake_at=runs.wake_at()
                        )
                    except StreamWake:
                        return
                    if delay is None:
                        raise
                else:
                    if stream_id is None:
                        raise RuntimeError("stream ended before event:published was received")
                    try:
                        delay = policy.after_end(wake_at=runs.wake_at())
                    except StreamWake:
                        return
                await _streaming.asleep(delay)
        except TimeoutError:
            # A failure held for its retry stands at the deadline; its
            # run:fail was yielded.
            if runs.wake_at() is None:
                raise

    async def _subscribe_with_event_once(
        self,
        body: Mapping[str, Any],
        *,
        project: str | None = None,
        deadline: float | None = None,
        wake: _streaming.Wake | None = None,
    ) -> AsyncGenerator[StreamEvent, None]:
        async with aclosing(
            aiter_stream(
                self._transport,
                "POST",
                "/streams/subscribe-with-event",
                json=dict(body),
                params={"project": project},
                deadline=deadline,
                wake=wake,
            )
        ) as events:
            async for event in events:
                yield event


class AsyncTasksResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, task_id: str) -> dict[str, Any]:
        """Return the latest durable task snapshot."""
        return await self._transport.request_data("GET", f"/tasks/{enc(task_id)}")

    async def subscribe(self, task_id: str) -> AsyncGenerator[StreamEvent, None]:
        """Yield durable task snapshots until the task becomes terminal."""
        async with aclosing(
            aiter_stream(
                self._transport,
                "GET",
                f"/tasks/{enc(task_id)}/subscribe",
            )
        ) as events:
            async for event in events:
                yield event

    async def wait(
        self,
        task_id: str,
        *,
        timeout: float = 60.0,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        """Wait for task completion; failed/cancelled/timed-out tasks raise HotTaskError."""
        policy = _snapshot_policy("task", timeout, max_attempts)
        while True:
            policy.connect()
            try:
                async with aclosing(
                    aiter_stream(
                        self._transport,
                        "GET",
                        f"/tasks/{enc(task_id)}/subscribe",
                        deadline=policy.deadline,
                    )
                ) as events:
                    async for event in events:
                        policy.saw(event)
                        task = _terminal_task(event)
                        if task is not None:
                            return task
            except Exception as error:
                delay = policy.after_error(error)
                if delay is None:
                    raise
            else:
                delay = policy.after_end()
            await _streaming.asleep(delay)


class AsyncFilesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the file list envelope (data + pagination)."""
        return await self._transport.request_json("GET", "/files", params=query)

    async def get(self, file_id: str) -> dict[str, Any]:
        """Return a single file's metadata."""
        return await self._transport.request_data("GET", f"/files/{enc(file_id)}")

    async def delete(self, file_id: str) -> None:
        """Delete a file."""
        await self._transport.request_json("DELETE", f"/files/{enc(file_id)}")

    async def download(self, file_id: str) -> httpx.Response:
        """Return the HTTP response with the whole file read into memory.

        Use :meth:`download_stream` for large files.
        """
        return await self._transport.request_raw("GET", f"/files/{enc(file_id)}/download")

    @asynccontextmanager
    async def download_stream(self, file_id: str) -> AsyncIterator[httpx.Response]:
        """Stream the file content without buffering it.

        ``async with hot.files.download_stream(file_id) as response:`` then
        iterate ``response.aiter_bytes()``.
        """
        async with self._transport.stream_raw("GET", f"/files/{enc(file_id)}/download") as response:
            yield response

    async def upload(
        self,
        path: str,
        content: bytes | str,
        *,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload content to a slash-separated file path."""
        response = await self._transport.request_raw(
            "PUT",
            f"/files/upload/{enc_path(path)}",
            content=content,
            headers={
                "Accept": "application/json",
                **({"Content-Type": content_type} if content_type else {}),
            },
        )
        return parse_data(response)

    async def initiate_upload(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Start a multipart upload."""
        return await self._transport.request_data("POST", "/files/uploads", json=dict(body))

    async def upload_part(
        self,
        upload_id: str,
        part_number: int,
        content: bytes | str,
    ) -> dict[str, Any]:
        """Upload one part of a multipart upload."""
        response = await self._transport.request_raw(
            "PUT",
            f"/files/uploads/{enc(upload_id)}/{part_number}",
            content=content,
            headers={"Accept": "application/json"},
        )
        return parse_data(response)

    async def complete_upload(self, upload_id: str) -> dict[str, Any]:
        """Finish a multipart upload."""
        return await self._transport.request_data(
            "POST",
            f"/files/uploads/{enc(upload_id)}/complete",
        )

    async def abort_upload(self, upload_id: str) -> None:
        """Cancel a multipart upload."""
        await self._transport.request_json("DELETE", f"/files/uploads/{enc(upload_id)}")


class AsyncBlobsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def download(self, blob_ref_id: str) -> httpx.Response:
        """Return the HTTP response with the whole blob read into memory.

        A large payload spilled to blob storage, in the caller's environment.
        Needs an environment API key; string payloads are served as UTF-8
        text. Use :meth:`download_stream` for large blobs.
        """
        return await self._transport.request_raw("GET", f"/blobs/{enc(blob_ref_id)}/download")

    @asynccontextmanager
    async def download_stream(self, blob_ref_id: str) -> AsyncIterator[httpx.Response]:
        """Stream the blob content without buffering it.

        ``async with hot.blobs.download_stream(blob_ref_id) as response:`` then
        iterate ``response.aiter_bytes()``.
        """
        async with self._transport.stream_raw(
            "GET", f"/blobs/{enc(blob_ref_id)}/download"
        ) as response:
            yield response


class AsyncProjectsResource:
    def __init__(self, transport: AsyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the project list envelope (data + pagination)."""
        return await self._transport.request_json("GET", f"{self._prefix}/projects", params=query)

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a project."""
        return await self._transport.request_data(
            "POST", f"{self._prefix}/projects", json=dict(body)
        )

    async def get(self, project: str) -> dict[str, Any]:
        """Return a project by id or slug."""
        return await self._transport.request_data("GET", f"{self._prefix}/projects/{enc(project)}")

    async def update(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Patch a project."""
        return await self._transport.request_data(
            "PATCH",
            f"{self._prefix}/projects/{enc(project)}",
            json=dict(body),
        )

    async def delete(self, project: str) -> None:
        """Delete a project."""
        await self._transport.request_json("DELETE", f"{self._prefix}/projects/{enc(project)}")

    async def activate(self, project: str) -> dict[str, Any]:
        """Activate a project."""
        return await self._transport.request_data(
            "POST", f"{self._prefix}/projects/{enc(project)}/activate"
        )

    async def deactivate(self, project: str) -> dict[str, Any]:
        """Deactivate a project."""
        return await self._transport.request_data(
            "POST", f"{self._prefix}/projects/{enc(project)}/deactivate"
        )

    async def event_handlers(self, project: str) -> dict[str, Any]:
        """Return the project's event handlers."""
        return await self._transport.request_json(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/event-handlers",
        )

    async def schedules(self, project: str) -> dict[str, Any]:
        """Return the project's schedules."""
        return await self._transport.request_json(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/schedules",
        )


class AsyncBuildsResource:
    def __init__(self, transport: AsyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the build list envelope across all projects."""
        return await self._transport.request_json("GET", f"{self._prefix}/builds", params=query)

    async def list_for_project(self, project: str, **query: Any) -> dict[str, Any]:
        """Return the build list envelope for one project."""
        return await self._transport.request_json(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds",
            params=query,
        )

    async def get(self, project: str, build_id: str) -> dict[str, Any]:
        """Return a single build."""
        return await self._transport.request_data(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}",
        )

    async def deployed(self, project: str) -> dict[str, Any]:
        """Return the project's deployed build."""
        return await self._transport.request_data(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/deployed",
        )

    async def live(self, project: str) -> dict[str, Any]:
        """Return the project's live build."""
        return await self._transport.request_data(
            "GET", f"{self._prefix}/projects/{enc(project)}/builds/live"
        )

    async def upload(
        self,
        project: str,
        *,
        file: Any,
        hash: str,
        build_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload a build archive as multipart form data."""
        data = {"hash": hash, **({"build_id": build_id} if build_id else {})}
        response = await self._transport.request_raw(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/builds",
            data=data,
            files={"file": file},
            headers={"Accept": "application/json"},
        )
        return parse_data(response)

    async def download(self, project: str, build_id: str) -> httpx.Response:
        """Return the HTTP response with the whole build archive read into memory.

        Use :meth:`download_stream` for large archives.
        """
        return await self._transport.request_raw(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/download",
        )

    @asynccontextmanager
    async def download_stream(self, project: str, build_id: str) -> AsyncIterator[httpx.Response]:
        """Stream the build archive without buffering it; iterate ``response.aiter_bytes()``."""
        async with self._transport.stream_raw(
            "GET",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/download",
        ) as response:
            yield response

    async def deploy(self, project: str, build_id: str) -> dict[str, Any]:
        """Deploy a build."""
        return await self._transport.request_data(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/builds/{enc(build_id)}/deploy",
        )


class AsyncContextResource:
    def __init__(self, transport: AsyncTransport, prefix: str = "") -> None:
        self._transport = transport
        self._prefix = prefix

    async def list(self, project: str) -> dict[str, Any]:
        """Return the context variable list envelope for a project."""
        return await self._transport.request_json(
            "GET", f"{self._prefix}/projects/{enc(project)}/context"
        )

    async def create(self, project: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a context variable."""
        return await self._transport.request_data(
            "POST",
            f"{self._prefix}/projects/{enc(project)}/context",
            json=dict(body),
        )

    async def update(self, project: str, key: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Replace a context variable."""
        return await self._transport.request_data(
            "PUT",
            f"{self._prefix}/projects/{enc(project)}/context/{enc(key)}",
            json=dict(body),
        )

    async def delete(self, project: str, key: str) -> None:
        """Delete a context variable."""
        await self._transport.request_json(
            "DELETE",
            f"{self._prefix}/projects/{enc(project)}/context/{enc(key)}",
        )


class AsyncDomainsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Register a custom domain."""
        return await self._transport.request_data("POST", "/domains", json=dict(body))

    async def list(self) -> dict[str, Any]:
        """Return the domain list envelope."""
        return await self._transport.request_json("GET", "/domains")

    async def get(self, domain_id: str) -> dict[str, Any]:
        """Return a single domain."""
        return await self._transport.request_data("GET", f"/domains/{enc(domain_id)}")

    async def delete(self, domain_id: str) -> None:
        """Delete a domain."""
        await self._transport.request_json("DELETE", f"/domains/{enc(domain_id)}")

    async def verify(self, domain_id: str) -> dict[str, Any]:
        """Trigger domain verification."""
        return await self._transport.request_data("POST", f"/domains/{enc(domain_id)}/verify")


class AsyncSessionsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a scoped session."""
        return await self._transport.request_data("POST", "/sessions", json=dict(body))

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the session list envelope."""
        return await self._transport.request_json("GET", "/sessions", params=query)

    async def revoke(self, session_id: str) -> None:
        """Revoke one session."""
        await self._transport.request_json("DELETE", f"/sessions/{enc(session_id)}")

    async def revoke_all(self) -> dict[str, Any]:
        """Revoke every session."""
        return await self._transport.request_data("DELETE", "/sessions")


class AsyncServiceKeysResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a scoped service key."""
        return await self._transport.request_data("POST", "/service-keys", json=dict(body))

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the service key list envelope."""
        return await self._transport.request_json("GET", "/service-keys", params=query)

    async def get(self, service_key_id: str) -> dict[str, Any]:
        """Return a single service key."""
        return await self._transport.request_data(
            "GET",
            f"/service-keys/{enc(service_key_id)}",
        )

    async def update(self, service_key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Patch a service key."""
        return await self._transport.request_data(
            "PATCH",
            f"/service-keys/{enc(service_key_id)}",
            json=dict(body),
        )

    async def revoke(self, service_key_id: str) -> None:
        """Revoke one service key."""
        await self._transport.request_json("DELETE", f"/service-keys/{enc(service_key_id)}")

    async def revoke_all(self) -> dict[str, Any]:
        """Revoke every service key."""
        return await self._transport.request_data("DELETE", "/service-keys")


class AsyncOrgEnv:
    """Async variant of :class:`OrgEnv`."""

    def __init__(self, transport: AsyncTransport, org_id: str, env_id: str) -> None:
        prefix = f"/orgs/{enc(org_id)}/envs/{enc(env_id)}"
        self.org_id = org_id
        self.env_id = env_id
        self.projects = AsyncProjectsResource(transport, prefix)
        self.builds = AsyncBuildsResource(transport, prefix)
        self.context = AsyncContextResource(transport, prefix)


class AsyncOrgsResource:
    """Async variant of :class:`OrgsResource`. Requires an org API key."""

    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def list(self, **query: Any) -> dict[str, Any]:
        """Return the list envelope of sub-organizations the key may read."""
        return await self._transport.request_json("GET", "/orgs", params=query)

    async def create(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a sub-org with its default environment; see :meth:`OrgsResource.create`."""
        return await self._transport.request_data("POST", "/orgs", json=dict(body))

    async def get(self, org_id: str) -> dict[str, Any]:
        """Return one organization."""
        return await self._transport.request_data("GET", f"/orgs/{enc(org_id)}")

    async def update(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Set a sub-org's ``state``: ``active``, ``suspended`` or ``archived``. Returns the org."""
        return await self._transport.request_data("PATCH", f"/orgs/{enc(org_id)}", json=dict(body))

    async def usage(self, org_id: str) -> dict[str, Any]:
        """Return usage across the parent organization and its sub-orgs; pass the parent's ID."""
        return await self._transport.request_data("GET", f"/orgs/{enc(org_id)}/usage")

    async def purge_calls(self, org_id: str, *, include_sub_orgs: bool = False) -> dict[str, Any]:
        """Delete every call trace in the org's environments (and its sub-organizations')."""
        return await self._transport.request_data(
            "DELETE",
            f"/orgs/{enc(org_id)}/calls",
            params={"include_sub_orgs": "true" if include_sub_orgs else None},
        )

    async def list_envs(self, org_id: str) -> dict[str, Any]:
        """Return the environment list envelope for an org."""
        return await self._transport.request_json("GET", f"/orgs/{enc(org_id)}/envs")

    async def create_env(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create an environment; idempotent by exact ``name`` within the org."""
        return await self._transport.request_data(
            "POST", f"/orgs/{enc(org_id)}/envs", json=dict(body)
        )

    async def get_env(self, org_id: str, env_id: str) -> dict[str, Any]:
        """Return one environment."""
        return await self._transport.request_data("GET", f"/orgs/{enc(org_id)}/envs/{enc(env_id)}")

    async def list_api_keys(self, org_id: str, **query: Any) -> dict[str, Any]:
        """Return the list envelope of the org's own org API keys."""
        return await self._transport.request_json(
            "GET", f"/orgs/{enc(org_id)}/api-keys", params=query
        )

    async def create_api_key(self, org_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Mint an org API key; the response's ``token`` is returned only once."""
        return await self._transport.request_data(
            "POST", f"/orgs/{enc(org_id)}/api-keys", json=dict(body)
        )

    async def list_env_api_keys(self, org_id: str, env_id: str, **query: Any) -> dict[str, Any]:
        """Return the list envelope of an environment's API keys."""
        return await self._transport.request_json(
            "GET", f"/orgs/{enc(org_id)}/envs/{enc(env_id)}/api-keys", params=query
        )

    async def create_env_api_key(
        self, org_id: str, env_id: str, body: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Mint an environment API key; the response's ``token`` is returned only once."""
        return await self._transport.request_data(
            "POST",
            f"/orgs/{enc(org_id)}/envs/{enc(env_id)}/api-keys",
            json=dict(body),
        )

    def env(self, org_id: str, env_id: str) -> AsyncOrgEnv:
        """Projects, builds and context in one of the org's environments."""
        return AsyncOrgEnv(self._transport, org_id, env_id)


class AsyncApiKeysResource:
    """Async variant of :class:`ApiKeysResource`. Requires an org API key."""

    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, key_id: str) -> dict[str, Any]:
        """Return one API key (never its secret)."""
        return await self._transport.request_data("GET", f"/api-keys/{enc(key_id)}")

    async def update(self, key_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Replace a key's ``permissions``; they must fit inside the caller's own."""
        return await self._transport.request_data(
            "PATCH", f"/api-keys/{enc(key_id)}", json=dict(body)
        )

    async def revoke(self, key_id: str) -> None:
        """Revoke a key. Everything it issued stops on its next request."""
        await self._transport.request_json("DELETE", f"/api-keys/{enc(key_id)}")

    async def rotate(self, key_id: str, *, overlap_seconds: int | None = None) -> dict[str, Any]:
        """Issue a new secret for the same key; see :meth:`ApiKeysResource.rotate`."""
        body = {} if overlap_seconds is None else {"overlap_seconds": overlap_seconds}
        return await self._transport.request_data(
            "POST", f"/api-keys/{enc(key_id)}/rotate", json=body
        )
