"""SSE streams with absolute deadlines, and the reconnect policy every waiter shares."""

from __future__ import annotations

import asyncio
import contextlib
import random
import socket
import sys
import threading
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Generator, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any

import httpx

from ._transport import (
    AsyncTransport,
    SyncTransport,
    _normalize_path,
    raise_for_status,
    without_none,
)
from .errors import HotApiError, HotRunError
from .sse import consume_sse_blocks
from .types import StreamEvent

# Hot sends a keepalive comment every 15 s, so a stream silent for longer than
# this is a dead socket. Hitting it is a transient error.
STREAM_READ_TIMEOUT = 45.0
_CONNECT_TIMEOUT = 10.0
_WRITE_TIMEOUT = 30.0

_BACKOFF_BASE = 0.5
_BACKOFF_CAP = 10.0
_MAX_RETRY_AFTER = 30.0

# Caps on memory retained for unrelated / not-yet-correlated runs on shared streams.
_MAX_BUFFERED_CHUNKS = 10_000
_MAX_FOREIGN_RUNS = 10_000
_MAX_ORPHAN_EVENTS = 10_000

# How long a failed run whose retry was scheduled waits for its retry run to
# appear before the failure is reported.
RETRY_GRACE = 5.0

_RUN_EVENTS = ("run:start", "run:stop", "run:fail", "run:cancel")


class StreamDeadline(TimeoutError):
    """The caller's deadline passed while a stream was open."""


class StreamWake(Exception):  # noqa: N818 - a signal, not an error
    """A waiter's wake time came (see ``RunCorrelator.wake_at``) while a stream was open."""


Wake = Callable[[], "float | None"]


def _target(deadline: float | None, wake: Wake | None) -> float | None:
    """The earlier of the deadline and the waiter's wake time."""
    at = wake() if wake is not None else None
    if at is None or (deadline is not None and deadline < at):
        return deadline
    return at


def _raise_if_due(deadline: float | None, wake: Wake | None) -> None:
    """Raise ``StreamWake`` once a wake time (or, with one set, the deadline) has
    passed, else ``StreamDeadline`` once the deadline has."""
    at = wake() if wake is not None else None
    if at is not None and (time.monotonic() >= at or _expired(deadline)):
        raise StreamWake()
    if _expired(deadline):
        raise StreamDeadline("deadline passed while streaming")


def sleep(seconds: float) -> None:
    time.sleep(seconds)


async def asleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _stream_timeout(deadline: float | None, wake: Wake | None = None) -> httpx.Timeout:
    target = _target(deadline, wake)
    if target is None:
        return httpx.Timeout(
            connect=_CONNECT_TIMEOUT,
            read=STREAM_READ_TIMEOUT,
            write=_WRITE_TIMEOUT,
            pool=_CONNECT_TIMEOUT,
        )
    remaining = target - time.monotonic()
    if remaining <= 0:
        _raise_if_due(deadline, wake)
        raise StreamDeadline("deadline passed before the stream opened")
    # The read timeout also bounds the wait for response headers.
    return httpx.Timeout(
        connect=min(_CONNECT_TIMEOUT, remaining),
        read=min(STREAM_READ_TIMEOUT, remaining),
        write=min(_WRITE_TIMEOUT, remaining),
        pool=min(_CONNECT_TIMEOUT, remaining),
    )


class _Watchdog:
    """Shut the response's socket down at a target time, so a blocked read returns.

    Keepalives arrive every 15 s, so without this a read could outlive the
    deadline (or a waiter's wake time) by up to one keepalive interval.
    """

    def __init__(self, response: httpx.Response, target: float | None) -> None:
        self.fired = False
        self._lock = threading.Lock()
        self._cancelled = False
        self._generation = 0
        self._sock = _socket_of(response)
        self._timer: threading.Timer | None = None
        self._target: float | None = None
        self.rearm(target)

    def rearm(self, target: float | None) -> None:
        """Move the shutdown to ``target`` (``None``: no shutdown)."""
        with self._lock:
            if self._cancelled or target == self._target:
                return
            self._generation += 1
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._target = target
            if target is None:
                return
            generation = self._generation
            timer = threading.Timer(
                max(0.0, target - time.monotonic()),
                self._fire,
                args=(generation,),
            )
            timer.daemon = True
            self._timer = timer
            timer.start()

    def _fire(self, generation: int) -> None:
        with self._lock:
            if self._cancelled or generation != self._generation:
                return
            self.fired = True
            sock = self._sock
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            self._generation += 1
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._sock = None


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
    wake: Wake | None = None,
) -> dict[str, Any]:
    return {
        "method": method,
        "url": f"{transport.api_base_url}{_normalize_path(path)}",
        "headers": transport.headers(
            {
                "Accept": "text/event-stream",
                **({"Content-Type": "application/json"} if json is not None else {}),
            }
        ),
        "json": dict(json) if json is not None else None,
        "params": without_none(params),
        "timeout": _stream_timeout(deadline, wake),
    }


async def _enter_stream(
    stream_cm: AbstractAsyncContextManager[httpx.Response],
    target: float | None,
) -> httpx.Response:
    if target is None:
        return await stream_cm.__aenter__()
    remaining = target - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    if sys.version_info >= (3, 11):
        async with asyncio.timeout(remaining):
            return await stream_cm.__aenter__()
    return await asyncio.wait_for(stream_cm.__aenter__(), timeout=remaining)


async def _anext_chunk(chunks: AsyncIterator[str], target: float | None) -> str:
    if target is None:
        return await chunks.__anext__()
    remaining = target - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    if sys.version_info >= (3, 11):
        async with asyncio.timeout(remaining):
            return await chunks.__anext__()
    return await asyncio.wait_for(chunks.__anext__(), timeout=remaining)


def iter_stream(
    transport: SyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    deadline: float | None = None,
    wake: Wake | None = None,
) -> Generator[StreamEvent, None, None]:
    """Open an SSE request and yield decoded stream events.

    ``deadline`` is an absolute ``time.monotonic()`` value. It bounds the
    connect, the wait for headers and every read; past it the stream raises
    ``StreamDeadline``, even while keepalives keep arriving. ``wake``
    returns the consumer's current wake time, if any; once it passes the
    stream raises ``StreamWake``.
    """
    _raise_if_due(deadline, wake)
    try:
        with transport.client.stream(
            **_request_kwargs(transport, method, path, json, params, deadline, wake)
        ) as response:
            if not response.is_success:
                response.read()
            raise_for_status(response)
            watchdog = _Watchdog(response, _target(deadline, wake))
            try:
                buffer = ""
                for chunk in response.iter_text():
                    buffer += chunk
                    events, buffer = consume_sse_blocks(buffer)
                    yield from events
                    _raise_if_due(deadline, wake)
                    watchdog.rearm(_target(deadline, wake))
                if watchdog.fired:
                    _raise_if_due(deadline, wake)
                    raise StreamDeadline("deadline passed while streaming")
                if buffer:
                    events, _ = consume_sse_blocks(buffer, flush=True)
                    yield from events
            except httpx.TransportError as error:
                if watchdog.fired:
                    _raise_if_due(deadline, wake)
                    raise StreamDeadline("deadline passed while streaming") from error
                raise
            finally:
                watchdog.cancel()
    except httpx.TimeoutException:
        if _expired(_target(deadline, wake)):
            _raise_if_due(deadline, wake)
        raise


async def aiter_stream(
    transport: AsyncTransport,
    method: str,
    path: str,
    *,
    json: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    deadline: float | None = None,
    wake: Wake | None = None,
) -> AsyncGenerator[StreamEvent, None]:
    """Async variant of :func:`iter_stream`."""
    _raise_if_due(deadline, wake)
    try:
        stream_cm = transport.client.stream(
            **_request_kwargs(transport, method, path, json, params, deadline, wake)
        )
        try:
            response = await _enter_stream(stream_cm, _target(deadline, wake))
        except (TimeoutError, asyncio.TimeoutError) as error:
            _raise_if_due(deadline, wake)
            raise StreamDeadline("deadline passed before the stream opened") from error
        try:
            if not response.is_success:
                await response.aread()
            raise_for_status(response)
            buffer = ""
            chunks = response.aiter_text()
            while True:
                try:
                    chunk = await _anext_chunk(chunks, _target(deadline, wake))
                except StopAsyncIteration:
                    break
                except (TimeoutError, asyncio.TimeoutError) as error:
                    _raise_if_due(deadline, wake)
                    raise StreamDeadline("deadline passed while streaming") from error
                buffer += chunk
                events, buffer = consume_sse_blocks(buffer)
                for event in events:
                    yield event
                _raise_if_due(deadline, wake)
            if buffer:
                events, _ = consume_sse_blocks(buffer, flush=True)
                for event in events:
                    yield event
        except BaseException as exc:
            await stream_cm.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            await stream_cm.__aexit__(None, None, None)
    except httpx.TimeoutException:
        if _expired(_target(deadline, wake)):
            _raise_if_due(deadline, wake)
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
    if retry_after is not None and retry_after > 0:
        delay = max(delay, min(float(retry_after), _MAX_RETRY_AFTER))
    return delay


class ReconnectPolicy:
    """When a durable waiter reconnects, and how long it waits first.

    - Any 4xx except 408 and 429 is permanent: the waiter raises it at once.
    - Transport errors, 408, 429 and 5xx reconnect with jittered exponential
      backoff, honouring ``retry_after``.
    - ``max_attempts`` counts consecutive failed connections. Explicit
      ``stream:complete`` rollovers reset the failure count and reconnect
      without delay; unexpected premature clean EOF backs off and counts as a
      failure.
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
        self._rolled_over = False

    def connect(self) -> None:
        """Start a connection; raises ``TimeoutError`` when the deadline has passed."""
        if _expired(self.deadline):
            raise TimeoutError(self.timeout_message)
        self._delivered = False
        self._rolled_over = False

    def saw(self, event: StreamEvent) -> None:
        self._delivered = True
        if event.get("type") == "stream:complete":
            self._rolled_over = True

    def after_error(
        self,
        error: BaseException,
        *,
        published: bool = True,
        wake_at: float | None = None,
    ) -> float | None:
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
        return self._bounded(backoff_delay(self.failures, retry_after), error, wake_at=wake_at)

    def after_end(self, *, wake_at: float | None = None) -> float:
        """Return the delay before reconnecting after a clean end of the stream."""
        if self._rolled_over:
            self.failures = 0
            return 0.0
        self.failures += 1
        if self.failures > self.max_attempts:
            raise RuntimeError(self.ended_message)
        return self._bounded(backoff_delay(self.failures), None, wake_at=wake_at)

    def _bounded(
        self,
        delay: float,
        error: BaseException | None,
        *,
        wake_at: float | None = None,
    ) -> float:
        now = time.monotonic()
        if wake_at is not None and now + delay >= wake_at:
            raise StreamWake()
        if self.deadline is not None and now + delay >= self.deadline:
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

    A run belongs to the event when its ``event_id`` is the event's, it was
    named as ``retry_run_id`` on a correlated run's ``run:fail``, or its
    ``origin_run_id`` names a run that does and its ``run_type`` is that run's:
    the retry of a failed attempt gets a new ``event_id``. A task run also
    names the run that started it as its origin, but has another ``run_type``.
    ``stream:data`` carries only a ``run_id``, and the server can deliver it
    (and ``run:stop``) before ``run:start``, so chunks for runs not yet
    correlated are held until a run event claims them.

    Hot replays a retried first attempt lossily: as ``run:fail`` or even
    ``run:stop`` with status ``pending_retry``, or, once the retry is queued
    and before the retry run exists, as ``run:fail`` with status ``failed``.
    So a ``pending_retry`` status is never final; a run that a retry run names
    as its origin (or whose ``run:fail`` names a ``retry_run_id``) is not final;
    and a ``failed`` run whose ``retry_attempt`` shows a scheduled retry is held
    for ``RETRY_GRACE`` seconds and reported (see :meth:`wake_at` and
    :meth:`expire`) only if no retry run appears.
    """

    def __init__(self, event_id: str | None, on_chunk: Callable[[str], None] | None = None) -> None:
        self.event_id = event_id
        self.on_chunk = on_chunk
        # Correlated runs and the highest retry_attempt seen for each.
        self._runs: dict[str, int] = {}
        # Each correlated run's run_type; a retry has its origin's.
        self._types: dict[str, Any] = {}
        # Recorded origin run for runs correlated via retry_run_id.
        self._origins: dict[str, str] = {}
        # Correlated runs that a correlated retry run names as its origin (or retry_run_id).
        self._retried: set[str] = set()
        # Run events whose origin run is not correlated yet, by origin_run_id.
        self._orphans: dict[str, list[tuple[str, dict[str, Any], str | None]]] = {}
        self._orphan_count = 0
        self._foreign: dict[str, None] = {}
        self._held: dict[str, list[StreamEvent]] = {}
        self._held_count = 0
        # A failure that stands unless its retry appears: (run_id, until, run).
        self._pending: tuple[str, float, dict[str, Any]] | None = None

    def observe(self, event: StreamEvent) -> tuple[str, dict[str, Any]] | None:
        """Track one event. When it ends the event's run, return the ending
        event type (``"run:stop"``, ``"run:fail"`` or ``"run:cancel"``) and run."""
        event_type = event.get("type")
        if event_type == "stream:data":
            self._chunk(event)
            return None
        if event_type not in _RUN_EVENTS:
            return None
        run = event.get("run")
        if not isinstance(run, dict) or not isinstance(run.get("run_id"), str):
            return None
        raw_retry = event.get("retry_run_id") if event_type == "run:fail" else None
        retry_run_id = raw_retry if isinstance(raw_retry, str) and raw_retry else None
        return self._run_event(event_type, run, retry_run_id=retry_run_id)

    def handle(self, event: StreamEvent) -> dict[str, Any] | None:
        """Return the run when the event's run succeeds; raise when it fails or is cancelled."""
        ended = self.observe(event)
        if ended is None:
            return None
        return self._result(*ended)

    def wake_at(self) -> float | None:
        """When a held failure stands (``time.monotonic()``), if one is held."""
        return self._pending[1] if self._pending is not None else None

    def expire(self) -> None:
        """Raise the held failure, if any: its retry did not appear in time."""
        if self._pending is not None:
            _, _, run = self._pending
            self._pending = None
            self._result("run:fail", run)

    def _result(self, event_type: str, run: dict[str, Any]) -> dict[str, Any]:
        if event_type == "run:fail":
            raise HotRunError(run, message=run_result_message(run, "Run failed"))
        if event_type == "run:cancel":
            raise HotRunError(run, message=run_result_message(run, "Run cancelled"))
        return run

    def _ties(self, run: Mapping[str, Any]) -> bool:
        run_id = run.get("run_id")
        if isinstance(run_id, str) and run_id in self._runs:
            return True
        if self.event_id is not None and run.get("event_id") == self.event_id:
            return True
        # A task run names the run that started it as its origin too, but
        # with another run_type; only a same-type run is a retry.
        origin = run.get("origin_run_id")
        return (
            isinstance(origin, str)
            and origin in self._runs
            and self._types[origin] == run.get("run_type")
        )

    def _record_foreign(self, run_id: str) -> None:
        if run_id not in self._foreign:
            if len(self._foreign) >= _MAX_FOREIGN_RUNS:
                self._foreign.pop(next(iter(self._foreign)))
            self._foreign[run_id] = None
        self._held_count -= len(self._held.pop(run_id, []))

    def _record_orphan(
        self, origin: str, event_type: str, run: dict[str, Any], retry_run_id: str | None
    ) -> None:
        while self._orphan_count >= _MAX_ORPHAN_EVENTS and self._orphans:
            oldest = next(iter(self._orphans))
            self._orphan_count -= len(self._orphans.pop(oldest))
        if _MAX_ORPHAN_EVENTS <= 0:
            return
        self._orphans.setdefault(origin, []).append((event_type, run, retry_run_id))
        self._orphan_count += 1

    def _register_run(
        self, run_id: str, attempt: int, run_type: Any
    ) -> list[tuple[str, dict[str, Any], str | None]]:
        self._foreign.pop(run_id, None)
        if run_id in self._runs:
            self._runs[run_id] = max(self._runs[run_id], attempt)
            if self._types[run_id] is None and run_type is not None:
                self._types[run_id] = run_type
            return []
        self._runs[run_id] = attempt
        self._types[run_id] = run_type
        for held in self._held.pop(run_id, []):
            self._held_count -= 1
            _emit_chunk(held, self.on_chunk)
        popped = self._orphans.pop(run_id, [])
        self._orphan_count -= len(popped)
        orphans = [orphan for orphan in popped if orphan[1].get("run_type") == self._types[run_id]]
        if orphans:
            # A retry of this run is already on the stream.
            self._retried.add(run_id)
        return orphans

    def _run_event(
        self,
        event_type: str,
        run: dict[str, Any],
        *,
        retry_run_id: str | None = None,
    ) -> tuple[str, dict[str, Any]] | None:
        run_id = run["run_id"]
        origin = run.get("origin_run_id")
        if not self._ties(run):
            if isinstance(origin, str) and origin not in self._runs:
                # A retry whose origin is not known yet may still turn out to be ours.
                self._record_orphan(origin, event_type, run, retry_run_id)
            elif self.event_id is not None:
                self._record_foreign(run_id)
            return None
        attempt = _retry_attempt(run)
        orphans = self._register_run(run_id, attempt, run.get("run_type"))
        if isinstance(origin, str):
            self._retried.add(origin)
            if self._pending is not None and self._pending[0] == origin:
                self._pending = None
        if retry_run_id is not None:
            self._retried.add(run_id)
            if self._pending is not None and self._pending[0] == run_id:
                self._pending = None
            self._origins.setdefault(retry_run_id, run_id)
            orphans.extend(
                self._register_run(retry_run_id, self._runs[run_id], self._types[run_id])
            )
        ended = self._outcome(event_type, run_id, run)
        if ended is not None:
            return ended
        for orphan_type, orphan_run, orphan_retry_run_id in orphans:
            ended = self._run_event(orphan_type, orphan_run, retry_run_id=orphan_retry_run_id)
            if ended is not None:
                return ended
        return None

    def _outcome(
        self, event_type: str, run_id: str, run: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        if run.get("status") == "pending_retry":
            return None
        if event_type in ("run:stop", "run:cancel"):
            return event_type, run
        if event_type != "run:fail" or run_id in self._retried:
            return None
        # retry_attempt goes up when a retry is scheduled; a run's own attempt
        # number is its origin's (0 for a first attempt).
        origin = run.get("origin_run_id") or self._origins.get(run_id)
        base = self._runs.get(origin, 0) if isinstance(origin, str) else 0
        if _retry_attempt(run) > base:
            if self._pending is None:
                self._pending = (run_id, time.monotonic() + RETRY_GRACE, run)
            return None
        return event_type, run

    def _chunk(self, event: StreamEvent) -> None:
        run_id = event.get("run_id")
        if not isinstance(run_id, str) or run_id in self._foreign:
            return
        if run_id in self._runs:
            _emit_chunk(event, self.on_chunk)
        elif self.on_chunk and self._held_count < _MAX_BUFFERED_CHUNKS:
            self._held.setdefault(run_id, []).append(event)
            self._held_count += 1


def _retry_attempt(run: Mapping[str, Any]) -> int:
    attempt = run.get("retry_attempt")
    return attempt if isinstance(attempt, int) and not isinstance(attempt, bool) else 0


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
