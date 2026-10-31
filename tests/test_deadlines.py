"""Waiter timeouts are absolute, against a real socket that keeps sending keepalives.

Hot sends a keepalive comment every 15 s. httpx read timeouts restart on every
read, so before 1.3.0 a waiter's timeout never fired while keepalives flowed.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from hot import AsyncHotClient, HotClient, _streaming

PUBLISHED = {"type": "event:published", "stream_id": "s1", "event_id": "e1"}


class SseServer:
    """Answer every request with an SSE stream that never ends.

    ``mode`` is ``"keepalive"`` (a comment every ``interval`` seconds),
    ``"silent"`` (headers, then nothing) or ``"no-headers"`` (accept, never reply).
    """

    def __init__(
        self, mode: str = "keepalive", interval: float = 0.2, events: tuple[dict, ...] = ()
    ) -> None:
        self.mode = mode
        self.interval = interval
        self.events = events
        self.connections = 0
        self._closing = threading.Event()
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args) -> None:
                pass

            def _serve(self) -> None:
                server.connections += 1
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                if server.mode == "no-headers":
                    server._closing.wait(30)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                try:
                    first = ((PUBLISHED,) if self.command == "POST" else ()) + server.events
                    for event in first:
                        self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                    self.wfile.flush()
                    while not server._closing.wait(server.interval):
                        if server.mode == "keepalive":
                            self.wfile.write(b": keepalive\n\n")
                            self.wfile.flush()
                except OSError:
                    return

            do_GET = do_POST = _serve

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.base_url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> SseServer:
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._closing.set()
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def keepalive_server() -> Iterator[SseServer]:
    with SseServer("keepalive") as server:
        yield server


SYNC_WAITERS = {
    "runs.wait": lambda hot, t: hot.runs.wait("r1", timeout=t),
    "tasks.wait": lambda hot, t: hot.tasks.wait("t1", timeout=t),
    "wait_for_run_result": lambda hot, t: hot.streams.wait_for_run_result("s1", "e1", timeout=t),
    "call_hot": lambda hot, t: hot.events.call_hot("::f/g", timeout=t),
    "subscribe_with_event": lambda hot, t: list(
        hot.streams.subscribe_with_event({"event_type": "x"}, timeout=t)
    ),
}


async def _collect(iterator) -> list:
    return [event async for event in iterator]


ASYNC_WAITERS = {
    "runs.wait": lambda hot, t: hot.runs.wait("r1", timeout=t),
    "tasks.wait": lambda hot, t: hot.tasks.wait("t1", timeout=t),
    "wait_for_run_result": lambda hot, t: hot.streams.wait_for_run_result("s1", "e1", timeout=t),
    "call_hot": lambda hot, t: hot.events.call_hot("::f/g", timeout=t),
    "subscribe_with_event": lambda hot, t: _collect(
        hot.streams.subscribe_with_event({"event_type": "x"}, timeout=t)
    ),
}


@pytest.mark.parametrize("waiter", list(SYNC_WAITERS))
def test_sync_timeout_fires_while_keepalives_flow(waiter, keepalive_server):
    started = time.monotonic()
    with HotClient(token="t", base_url=keepalive_server.base_url) as hot:
        with pytest.raises(TimeoutError):
            SYNC_WAITERS[waiter](hot, 1.0)
    assert 0.9 < time.monotonic() - started < 2.5


@pytest.mark.parametrize("waiter", list(ASYNC_WAITERS))
def test_async_timeout_fires_while_keepalives_flow(waiter, keepalive_server):
    async def scenario():
        async with AsyncHotClient(token="t", base_url=keepalive_server.base_url) as hot:
            await ASYNC_WAITERS[waiter](hot, 1.0)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(scenario())
    assert 0.9 < time.monotonic() - started < 2.5


def test_deadline_interrupts_a_read_between_keepalives():
    # Keepalives every 1.5 s keep resetting a 2 s read timeout; the deadline
    # still closes the stream at 2 s instead of at the next keepalive (3 s).
    with SseServer("keepalive", interval=1.5) as server:
        with HotClient(token="t", base_url=server.base_url) as hot:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                hot.runs.wait("r1", timeout=2.0)
            elapsed = time.monotonic() - started

        async def scenario():
            async with AsyncHotClient(token="t", base_url=server.base_url) as hot:
                await hot.runs.wait("r1", timeout=2.0)

        async_started = time.monotonic()
        with pytest.raises(TimeoutError):
            asyncio.run(scenario())
        async_elapsed = time.monotonic() - async_started
    assert 1.9 < elapsed < 2.7
    assert 1.9 < async_elapsed < 2.7


def test_timeout_covers_the_wait_for_response_headers():
    with SseServer("no-headers") as server:
        with HotClient(token="t", base_url=server.base_url) as hot:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                hot.tasks.wait("t1", timeout=1.0)
    assert time.monotonic() - started < 2.5


def test_dead_socket_is_a_transient_error(monkeypatch):
    monkeypatch.setattr(_streaming, "STREAM_READ_TIMEOUT", 0.3)
    monkeypatch.setattr(_streaming, "sleep", lambda _seconds: None)
    with SseServer("silent") as server:
        with HotClient(token="t", base_url=server.base_url) as hot:
            with pytest.raises(httpx.ReadTimeout):
                hot.streams.wait_for_run_result("s1", "e1", timeout=30, max_attempts=2)
        assert server.connections == 3


# hot replays a first attempt whose retry is queued as run:fail with status
# failed; the waiter holds it for RETRY_GRACE and reports it if no retry run
# appears, even while keepalives keep the stream open.
HELD_FAILURE = (
    {
        "type": "run:fail",
        "run": {
            "run_id": "r1",
            "event_id": "e1",
            "status": "failed",
            "retry_attempt": 1,
            "result": "first attempt",
        },
    },
)


def test_held_failure_is_reported_after_the_grace(monkeypatch):
    monkeypatch.setattr(_streaming, "RETRY_GRACE", 1.0)
    with SseServer("keepalive", interval=5.0, events=HELD_FAILURE) as server:
        with HotClient(token="t", base_url=server.base_url) as hot:
            started = time.monotonic()
            with pytest.raises(RuntimeError, match="first attempt"):
                hot.events.call_hot("::f/g", timeout=30)
            elapsed = time.monotonic() - started
            sub_started = time.monotonic()
            events = list(hot.streams.subscribe_with_event({"event_type": "x"}, timeout=30))
            sub_elapsed = time.monotonic() - sub_started

        async def scenario():
            async with AsyncHotClient(token="t", base_url=server.base_url) as hot:
                await hot.streams.wait_for_run_result("s1", "e1", timeout=30)

        async_started = time.monotonic()
        with pytest.raises(RuntimeError, match="first attempt"):
            asyncio.run(scenario())
        async_elapsed = time.monotonic() - async_started
    assert 0.9 < elapsed < 2.0
    assert [event["type"] for event in events] == ["event:published", "run:fail"]
    assert 0.9 < sub_elapsed < 2.0
    assert 0.9 < async_elapsed < 2.0


def test_held_failure_is_reported_at_an_earlier_deadline():
    with SseServer("keepalive", interval=5.0, events=HELD_FAILURE) as server:
        with HotClient(token="t", base_url=server.base_url) as hot:
            started = time.monotonic()
            with pytest.raises(RuntimeError, match="first attempt"):
                hot.streams.wait_for_run_result("s1", "e1", timeout=1.0)
    assert time.monotonic() - started < 2.0
