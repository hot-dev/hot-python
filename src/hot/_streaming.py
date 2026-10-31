"""SSE streams with absolute deadlines, and the reconnect policy every waiter shares."""

from __future__ import annotations

import asyncio
import contextlib
import random
import socket
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any

import httpx

from ._transport import AsyncTransport, SyncTransport, raise_for_status, without_none
from .errors import HotApiError
from .sse import consume_sse_blocks
from .types import StreamEvent

# Hot sends a keepalive comment every 15 s, so a stream silent for longer than
# this is a dead socket. Hitting it is a transient error.
STREAM_READ_TIMEOUT = 45.0
_CONNECT_TIMEOUT = 10.0
_WRITE_TIMEOUT = 30.0

_BACKOFF_BASE = 0.5
_BACKOFF_CAP = 10.0
_MAX_RETRY_AFTER = 60.0

# stream:data chunks held for runs that are not yet correlated.
_MAX_BUFFERED_CHUNKS = 10_000

_RUN_EVENTS = ("run:start", "run:stop", "run:fail", "run:cancel")


class StreamDeadline(TimeoutError):
    """The caller's deadline passed while a stream was open."""


def sleep(seconds: float) -> None:
    time.sleep(seconds)


async def asleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _stream_timeout(deadline: float | None) -> httpx.Timeout:
    if deadline is None:
        return httpx.Timeout(
            connect=_CONNECT_TIMEOUT,
            read=STREAM_READ_TIMEOUT,
            write=_WRITE_TIMEOUT,
            pool=_CONNECT_TIMEOUT,
        )
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise StreamDeadline("deadline passed before the stream opened")
    # The read timeout also bounds the wait for response headers.
    return httpx.Timeout(
        connect=min(_CONNECT_TIMEOUT, remaining),
        read=min(STREAM_READ_TIMEOUT, remaining),
        write=min(_WRITE_TIMEOUT, remaining),
        pool=min(_CONNECT_TIMEOUT, remaining),
    )


class _Watchdog:
    """Shut the response's socket down at the deadline, so a blocked read returns.

    Keepalives arrive every 15 s, so without this a read could outlive the
    deadline by up to one keepalive interval.
    """

    def __init__(self, response: httpx.Response, deadline: float | None) -> None:
        self.fired = False
        self._timer: threading.Timer | None = None
        if deadline is None:
            return
        sock = _socket_of(response)
        self._timer = threading.Timer(
            max(0.0, deadline - time.monotonic()), self._fire, args=(sock,)
        )
        self._timer.daemon = True
        self._timer.start()

    def _fire(self, sock: socket.socket | None) -> None:
        self.fired = True
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

    def cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()


def _socket_of(response: httpx.Response) -> socket.socket | None:
    stream = response.extensions.get("network_stream")
    if stream is None:
        return None
    try:
        sock = stream.get_extra_info("socket")
    except Exception:  # pragma: no cover - backend specific
        return None
    return sock if hasattr(sock, "shutdown") else None


def _request_kwargs(
    transport: SyncTransport | AsyncTransport,
    method: str,
    path: str,
    json: Mapping[str, Any] | None,
    params: Mapping[str, Any] | None,
    deadline: float | None,
) -> dict[str, Any]:
    return {
        "method": method,
        "url": f"{transport.api_base_url}{path}",
        "headers": transport.headers(
            {
                "Accept": "text/event-stream",
                **({"Content-Type": "application/json"} if json is not None else {}),
            }
        ),
        "json": dict(json) if json is not None else None,
        "params": without_none(params),
        "timeout": _stream_timeout(deadline),
    }


def iter_stream(
    transport: SyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    deadline: float | None = None,
) -> Iterator[StreamEvent]:
    """Open an SSE request and yield decoded stream events.

    ``deadline`` is an absolute ``time.monotonic()`` value. It bounds the
    connect, the wait for headers and every read; past it the stream raises
    ``StreamDeadline``, even while keepalives keep arriving.
    """
    try:
        with transport.client.stream(
            **_request_kwargs(transport, method, path, json, params, deadline)
        ) as response:
            if response.is_error:
                response.read()
            raise_for_status(response)
            watchdog = _Watchdog(response, deadline)
            try:
                buffer = ""
                for chunk in response.iter_text():
                    buffer += chunk
                    events, buffer = consume_sse_blocks(buffer)
                    yield from events
                    if _expired(deadline):
                        raise StreamDeadline("deadline passed while streaming")
                if watchdog.fired:
                    raise StreamDeadline("deadline passed while streaming")
                events, _ = consume_sse_blocks(f"{buffer}\n\n")
                yield from events
            except httpx.TransportError as error:
                if watchdog.fired:
                    raise StreamDeadline("deadline passed while streaming") from error
                raise
            finally:
                watchdog.cancel()
    except httpx.TimeoutException as error:
        if _expired(deadline):
            raise StreamDeadline("deadline passed while streaming") from error
        raise


async def aiter_stream(
    transport: AsyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    deadline: float | None = None,
) -> AsyncIterator[StreamEvent]:
    """Async variant of :func:`iter_stream`."""
    try:
        async with transport.client.stream(
            **_request_kwargs(transport, method, path, json, params, deadline)
        ) as response:
            if response.is_error:
                await response.aread()
            raise_for_status(response)
            watchdog = _Watchdog(response, deadline)
            try:
                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    events, buffer = consume_sse_blocks(buffer)
                    for event in events:
                        yield event
                    if _expired(deadline):
                        raise StreamDeadline("deadline passed while streaming")
                if watchdog.fired:
                    raise StreamDeadline("deadline passed while streaming")
                events, _ = consume_sse_blocks(f"{buffer}\n\n")
                for event in events:
                    yield event
            except httpx.TransportError as error:
                if watchdog.fired:
                    raise StreamDeadline("deadline passed while streaming") from error
                raise
            finally:
                watchdog.cancel()
    except httpx.TimeoutException as error:
        if _expired(deadline):
            raise StreamDeadline("deadline passed while streaming") from error
        raise


def is_transient(error: BaseException) -> bool:
    """Whether a reconnect could succeed: transport errors, 408, 429 and 5xx."""
    if isinstance(error, HotApiError):
        return error.status_code in (408, 429) or error.status_code >= 500
    return isinstance(
        error,
        (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.ProxyError),
    )


def backoff_delay(failures: int, retry_after: float | None = None) -> float:
    """Jittered exponential backoff before consecutive failure ``failures``."""
    ceiling = min(_BACKOFF_CAP, _BACKOFF_BASE * 2 ** (failures - 1))
    delay = ceiling / 2 + random.uniform(0, ceiling / 2)
    if retry_after:
        delay = max(delay, min(float(retry_after), _MAX_RETRY_AFTER))
    return delay


class ReconnectPolicy:
    """When a durable waiter reconnects, and how long it waits first.

    - Any 4xx except 408 and 429 is permanent: the waiter raises it at once.
    - Transport errors, 408, 429 and 5xx reconnect with jittered exponential
      backoff, honouring ``retry_after``.
    - ``max_attempts`` counts consecutive failed connections. A connection that
      delivered an event (including the server's 5-minute ``stream:complete``
      rollover) resets the count and reconnects without delay; a clean end with
      no events counts as a failure.
    - ``timeout`` is an absolute deadline over connects, reads and backoff.
      A backoff that would cross it raises ``TimeoutError`` instead.
    """

    def __init__(
        self,
        *,
        timeout: float | None,
        max_attempts: int,
        timeout_message: str,
        ended_message: str,
    ) -> None:
        self.deadline = None if timeout is None else time.monotonic() + timeout
        self.max_attempts = max_attempts
        self.timeout_message = timeout_message
        self.ended_message = ended_message
        self.failures = 0
        self._delivered = False

    def connect(self) -> None:
        """Start a connection; raises ``TimeoutError`` when the deadline has passed."""
        if _expired(self.deadline):
            raise TimeoutError(self.timeout_message)
        self._delivered = False

    def saw(self, _event: StreamEvent) -> None:
        self._delivered = True

    def after_error(self, error: BaseException, *, published: bool = True) -> float | None:
        """Return the delay before reconnecting, or ``None`` to re-raise ``error``.

        ``published=False`` means the request that publishes an event failed,
        so only a 429 or 503 rejection (the event was not published) is retried.
        """
        if isinstance(error, StreamDeadline):
            raise TimeoutError(self.timeout_message) from error
        if not is_transient(error):
            return None
        if not published and not (
            isinstance(error, HotApiError) and error.status_code in (429, 503)
        ):
            return None
        if self._delivered:
            self.failures = 0
        self.failures += 1
        if self.failures > self.max_attempts:
            return None
        retry_after = error.retry_after if isinstance(error, HotApiError) else None
        return self._bounded(backoff_delay(self.failures, retry_after), error)

    def after_end(self) -> float:
        """Return the delay before reconnecting after a clean end of the stream."""
        if self._delivered:
            self.failures = 0
            return 0.0
        self.failures += 1
        if self.failures > self.max_attempts:
            raise RuntimeError(self.ended_message)
        return self._bounded(backoff_delay(self.failures), None)

    def _bounded(self, delay: float, error: BaseException | None) -> float:
        if self.deadline is not None and time.monotonic() + delay >= self.deadline:
            raise TimeoutError(self.timeout_message) from error
        return delay


def _emit_chunk(event: StreamEvent, on_chunk: Callable[[str], None] | None) -> None:
    if not on_chunk:
        return
    payload = event.get("payload")
    if isinstance(payload, dict):
        text = payload.get("text")
        if isinstance(text, str):
            on_chunk(text)


class RunCorrelator:
    """Follow the runs a published event starts on its stream, in any arrival order.

    A run belongs to the event when its ``event_id`` is the event's, or its
    ``origin_run_id`` names a run that does: the retry of a ``pending_retry``
    failure gets a new ``event_id``. ``stream:data`` carries only a ``run_id``,
    and the server can deliver it (and ``run:stop``) before ``run:start``, so
    chunks for runs not yet correlated are held until a run event claims them.
    """

    def __init__(self, event_id: str | None, on_chunk: Callable[[str], None] | None = None):
        self.event_id = event_id
        self.on_chunk = on_chunk
        self._runs: set[str] = set()
        self._foreign: set[str] = set()
        self._held: dict[str, list[StreamEvent]] = {}
        self._held_count = 0

    def observe(self, event: StreamEvent) -> str | None:
        """Track one event. Return ``"run:stop"``, ``"run:fail"`` or ``"run:cancel"``
        when it ends the event's run; a ``pending_retry`` failure does not."""
        event_type = event.get("type")
        if event_type == "stream:data":
            self._chunk(event)
            return None
        if event_type not in _RUN_EVENTS:
            return None
        run = event.get("run")
        if not isinstance(run, dict) or not self._correlate(run):
            return None
        if event_type == "run:fail" and run.get("status") == "pending_retry":
            return None
        return event_type if event_type != "run:start" else None

    def handle(self, event: StreamEvent) -> dict[str, Any] | None:
        """Return the run when the event's run succeeds; raise when it fails or is cancelled."""
        ended = self.observe(event)
        if ended is None:
            return None
        run = event["run"]
        if ended == "run:fail":
            raise RuntimeError(run_result_message(run, "Run failed"))
        if ended == "run:cancel":
            raise RuntimeError(run_result_message(run, "Run cancelled"))
        return run

    def _correlate(self, run: Mapping[str, Any]) -> bool:
        run_id = run.get("run_id")
        if not isinstance(run_id, str):
            return False
        if run_id in self._runs:
            return True
        if self.event_id is None:
            return False
        origin = run.get("origin_run_id")
        if run.get("event_id") == self.event_id or (
            isinstance(origin, str) and origin in self._runs
        ):
            self._runs.add(run_id)
            for held in self._held.pop(run_id, []):
                self._held_count -= 1
                _emit_chunk(held, self.on_chunk)
            return True
        # A retry whose origin is not known yet may still turn out to be ours.
        if not isinstance(origin, str):
            self._foreign.add(run_id)
            self._held_count -= len(self._held.pop(run_id, []))
        return False

    def _chunk(self, event: StreamEvent) -> None:
        run_id = event.get("run_id")
        if not isinstance(run_id, str) or run_id in self._foreign:
            return
        if run_id in self._runs:
            _emit_chunk(event, self.on_chunk)
        elif self.on_chunk and self._held_count < _MAX_BUFFERED_CHUNKS:
            self._held.setdefault(run_id, []).append(event)
            self._held_count += 1


def _result_message(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if not isinstance(value, dict):
        return None
    for key in ("$err", "$val", "error", "message", "msg", "reason", "err"):
        message = _result_message(value.get(key))
        if message:
            return message
    return None


def run_result_message(run: Mapping[str, Any], fallback: str) -> str:
    message = _result_message(run.get("result"))
    if message:
        return message
    status = run.get("status")
    return f"{fallback} ({status})" if isinstance(status, str) and status else fallback
