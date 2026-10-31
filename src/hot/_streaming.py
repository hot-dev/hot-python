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


def _stream_timeout(deadline: float | None) -> httpx.Timeout:
    # ``deadline`` here is the earlier of the deadline and any wake time.
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
    """Shut the response's socket down at a target time, so a blocked read returns.

    Keepalives arrive every 15 s, so without this a read could outlive the
    deadline (or a waiter's wake time) by up to one keepalive interval.
    """

    def __init__(self, response: httpx.Response, target: float | None) -> None:
        self.fired = False
        self._sock = _socket_of(response)
        self._timer: threading.Timer | None = None
        self._target: float | None = None
        self.rearm(target)

    def rearm(self, target: float | None) -> None:
        """Move the shutdown to ``target`` (``None``: no shutdown)."""
        if target == self._target:
            return
        self.cancel()
        self._target = target
        if target is None:
            return
        self._timer = threading.Timer(
            max(0.0, target - time.monotonic()), self._fire, args=(self._sock,)
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
            self._timer = None


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
    wake: Wake | None = None,
) -> Iterator[StreamEvent]:
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
            **_request_kwargs(transport, method, path, json, params, _target(deadline, wake))
        ) as response:
            if response.is_error:
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
                events, _ = consume_sse_blocks(f"{buffer}\n\n")
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
) -> AsyncIterator[StreamEvent]:
    """Async variant of :func:`iter_stream`."""
    _raise_if_due(deadline, wake)
    try:
        async with transport.client.stream(
            **_request_kwargs(transport, method, path, json, params, _target(deadline, wake))
        ) as response:
            if response.is_error:
                await response.aread()
            raise_for_status(response)
            watchdog = _Watchdog(response, _target(deadline, wake))
            try:
                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    events, buffer = consume_sse_blocks(buffer)
                    for event in events:
                        yield event
                    _raise_if_due(deadline, wake)
                    watchdog.rearm(_target(deadline, wake))
                if watchdog.fired:
                    _raise_if_due(deadline, wake)
                    raise StreamDeadline("deadline passed while streaming")
                events, _ = consume_sse_blocks(f"{buffer}\n\n")
                for event in events:
                    yield event
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
    as its origin is not final; and a ``failed`` run whose ``retry_attempt``
    shows a scheduled retry is held for ``RETRY_GRACE`` seconds and reported
    (see :meth:`wake_at` and :meth:`expire`) only if no retry run appears.
    """

    def __init__(self, event_id: str | None, on_chunk: Callable[[str], None] | None = None):
        self.event_id = event_id
        self.on_chunk = on_chunk
        # Correlated runs and the highest retry_attempt seen for each.
        self._runs: dict[str, int] = {}
        # Each correlated run's run_type; a retry has its origin's.
        self._types: dict[str, Any] = {}
        # Correlated runs that a correlated retry run names as its origin.
        self._retried: set[str] = set()
        # Run events whose origin run is not correlated yet, by origin_run_id.
        self._orphans: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        self._foreign: set[str] = set()
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
        return self._run_event(event_type, run)

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
            raise RuntimeError(run_result_message(run, "Run failed"))
        if event_type == "run:cancel":
            raise RuntimeError(run_result_message(run, "Run cancelled"))
        return run

    def _ties(self, run: Mapping[str, Any]) -> bool:
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

    def _run_event(self, event_type: str, run: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
        run_id = run["run_id"]
        origin = run.get("origin_run_id")
        if not self._ties(run):
            if isinstance(origin, str) and origin not in self._runs:
                # A retry whose origin is not known yet may still turn out to be ours.
                self._orphans.setdefault(origin, []).append((event_type, run))
            elif self.event_id is not None:
                self._foreign.add(run_id)
                self._held_count -= len(self._held.pop(run_id, []))
            return None
        attempt = _retry_attempt(run)
        orphans: list[tuple[str, dict[str, Any]]] = []
        if run_id in self._runs:
            self._runs[run_id] = max(self._runs[run_id], attempt)
        else:
            self._runs[run_id] = attempt
            self._types[run_id] = run.get("run_type")
            for held in self._held.pop(run_id, []):
                self._held_count -= 1
                _emit_chunk(held, self.on_chunk)
            orphans = [
                (orphan_type, orphan)
                for orphan_type, orphan in self._orphans.pop(run_id, [])
                if orphan.get("run_type") == self._types[run_id]
            ]
            if orphans:
                # A retry of this run is already on the stream.
                self._retried.add(run_id)
        if isinstance(origin, str):
            self._retried.add(origin)
            if self._pending is not None and self._pending[0] == origin:
                self._pending = None
        ended = self._outcome(event_type, run_id, run)
        if ended is not None:
            return ended
        for orphan_type, orphan in orphans:
            ended = self._run_event(orphan_type, orphan)
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
        origin = run.get("origin_run_id")
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
