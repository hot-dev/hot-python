from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest

from hot import AsyncHotClient, HotApiError, HotClient, HotRunError, HotTaskError, _streaming
from hot._transport import DEFAULT_TIMEOUT, SyncTransport
from hot.errors import parse_api_error


def counting_body(chunks: int, consumed: list[int]):
    def body():
        for index in range(chunks):
            consumed.append(index)
            yield b"x" * 1024

    return body()


def test_download_stream_does_not_buffer_the_body():
    consumed: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path in (
            "/v1/files/f1/download",
            "/v1/projects/p1/builds/b1/download",
        )
        return httpx.Response(200, content=counting_body(64, consumed))

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with hot.files.download_stream("f1") as response:
            chunks = response.iter_raw()
            assert len(next(chunks)) == 1024
            assert len(consumed) < 4
            assert sum(len(chunk) for chunk in chunks) == 63 * 1024
        consumed.clear()
        with hot.builds.download_stream("p1", "b1") as response:
            next(response.iter_raw())
            assert len(consumed) < 4


def test_async_download_stream_does_not_buffer_the_body():
    consumed: list[int] = []

    async def body():
        for index in range(64):
            consumed.append(index)
            yield b"x" * 1024

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body())

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as hot:
            async with hot.files.download_stream("f1") as response:
                async for _chunk in response.aiter_raw():
                    break
                assert len(consumed) < 4
            consumed.clear()
            async with hot.orgs.env("o1", "e1").builds.download_stream("p1", "b1") as response:
                assert response.request.url.path == (
                    "/v1/orgs/o1/envs/e1/projects/p1/builds/b1/download"
                )

    asyncio.run(scenario())


def test_download_stream_raises_api_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "gone"}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError) as error:
            with hot.files.download_stream("f1"):
                pass
    assert error.value.status_code == 404


def test_default_timeout_is_finite():
    with HotClient(token="t") as hot:
        assert hot._transport.client.timeout == DEFAULT_TIMEOUT
    with HotClient(token="t", timeout=httpx.Timeout(None)) as hot:
        assert hot._transport.client.timeout == httpx.Timeout(None)


def test_idempotent_requests_retry_502_503_504(monkeypatch):
    monkeypatch.setattr("hot._transport.time.sleep", lambda _seconds: None)
    statuses = [503, 502, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        if status != 200:
            return httpx.Response(status, json={"error": {"code": "unavailable", "message": "x"}})
        return httpx.Response(200, json={"data": {"run_id": "r1"}, "meta": {}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        assert hot.runs.get("r1") == {"run_id": "r1"}


def test_non_idempotent_requests_do_not_retry_5xx(monkeypatch):
    monkeypatch.setattr("hot._transport.time.sleep", lambda _seconds: None)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(503, json={"error": {"code": "unavailable", "message": "x"}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError):
            hot.events.publish({"event_type": "x", "event_data": {}})
    assert calls == ["POST"]


@pytest.mark.parametrize("status", [302, 307])
def test_redirect_responses_raise_hot_api_error_sync_and_async(status: int):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            text="Moved",
            headers={"Location": "https://other.example.test/"},
        )

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError) as json_err:
            hot.runs.get("r1")
        assert json_err.value.status_code == status

        with pytest.raises(HotApiError) as raw_err:
            hot.files.download("f1")
        assert raw_err.value.status_code == status

        with pytest.raises(HotApiError) as stream_raw_err:
            with hot.files.download_stream("f1"):
                pass
        assert stream_raw_err.value.status_code == status

        with pytest.raises(HotApiError) as sse_err:
            list(hot.streams.subscribe("s1"))
        assert sse_err.value.status_code == status

    async def scenario() -> None:
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as ahot:
            with pytest.raises(HotApiError) as ajson_err:
                await ahot.runs.get("r1")
            assert ajson_err.value.status_code == status

            with pytest.raises(HotApiError) as araw_err:
                await ahot.files.download("f1")
            assert araw_err.value.status_code == status

            with pytest.raises(HotApiError) as astream_err:
                async with ahot.files.download_stream("f1"):
                    pass
            assert astream_err.value.status_code == status

            with pytest.raises(HotApiError) as asse_err:
                [event async for event in ahot.streams.subscribe("s1")]
            assert asse_err.value.status_code == status

    asyncio.run(scenario())


def test_events_publish_with_event_id_503_in_progress_and_409_conflict(monkeypatch):
    sleeps: list[float] = []

    async def fake_asleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("hot._transport.time.sleep", sleeps.append)
    monkeypatch.setattr("hot._transport.asyncio.sleep", fake_asleep)

    # 1) 503 event_publish_in_progress retries with retry_after (capped at 30s) + fallback backoff.
    responses = [
        httpx.Response(
            503,
            json={
                "error": {
                    "code": "event_publish_in_progress",
                    "message": "Publish already in progress",
                    "retry_after": 60,
                }
            },
        ),
        httpx.Response(
            503,
            json={
                "error": {
                    "code": "event_publish_in_progress",
                    "message": "Still in progress",
                }
            },
        ),
        httpx.Response(
            200,
            json={"data": {"event_id": "evt_idem_1", "stream_id": "s1"}, "meta": {}},
        ),
    ]
    bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return responses.pop(0)

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(timeout=60.0, transport=httpx.MockTransport(handler)),
    ) as hot:
        ev = hot.events.publish(
            {"event_type": "x", "event_data": {}, "event_id": "evt_idem_1"},
        )
    assert ev == {"event_id": "evt_idem_1", "stream_id": "s1"}
    assert len(bodies) == 3
    assert all(b["event_id"] == "evt_idem_1" for b in bodies)
    assert len(sleeps) == 2
    assert sleeps[0] == 30.0
    assert 0.5 <= sleeps[1] <= 1.0

    # 2) Async 503 event_publish_in_progress retry.
    sleeps.clear()
    async_responses = [
        httpx.Response(
            503,
            json={
                "error": {
                    "code": "event_publish_in_progress",
                    "message": "in progress",
                    "retry_after": 2,
                }
            },
        ),
        httpx.Response(
            200,
            json={"data": {"event_id": "evt_idem_2", "stream_id": "s2"}, "meta": {}},
        ),
    ]

    async def async_retry_scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(
                timeout=60.0, transport=httpx.MockTransport(lambda _: async_responses.pop(0))
            ),
        ) as ahot:
            return await ahot.events.publish(
                {"event_type": "x", "event_data": {}, "event_id": "evt_idem_2"}
            )

    assert asyncio.run(async_retry_scenario())["event_id"] == "evt_idem_2"
    assert len(sleeps) == 1 and 2.0 <= sleeps[0] <= 2.5

    # 3) 409 event_id_conflict is NOT retried (sync & async).
    conflict_calls = 0

    def conflict_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal conflict_calls
        conflict_calls += 1
        return httpx.Response(
            409,
            json={"error": {"code": "event_id_conflict", "message": "Different payload"}},
        )

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(conflict_handler)),
    ) as hot:
        with pytest.raises(HotApiError) as err:
            hot.events.publish({"event_type": "x", "event_data": {}, "event_id": "evt_dup"})
    assert err.value.status_code == 409
    assert err.value.code == "event_id_conflict"
    assert conflict_calls == 1

    async def async_conflict_scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(conflict_handler)),
        ) as ahot:
            with pytest.raises(HotApiError) as aerr:
                await ahot.events.publish(
                    {"event_type": "x", "event_data": {}, "event_id": "evt_dup"}
                )
            assert aerr.value.status_code == 409
            assert aerr.value.code == "event_id_conflict"

    asyncio.run(async_conflict_scenario())
    assert conflict_calls == 2


@pytest.mark.parametrize(
    "call_sync,call_async",
    [
        (lambda h: h.projects.delete("p1"), lambda h: h.projects.delete("p1")),
        (lambda h: h.sessions.revoke("s1"), lambda h: h.sessions.revoke("s1")),
        (lambda h: h.files.delete("f1"), lambda h: h.files.delete("f1")),
        (lambda h: h.files.abort_upload("u1"), lambda h: h.files.abort_upload("u1")),
        (lambda h: h.context.delete("p1", "K"), lambda h: h.context.delete("p1", "K")),
        (lambda h: h.domains.delete("d1"), lambda h: h.domains.delete("d1")),
        (lambda h: h.service_keys.revoke("sk1"), lambda h: h.service_keys.revoke("sk1")),
        (lambda h: h.api_keys.revoke("k1"), lambda h: h.api_keys.revoke("k1")),
    ],
    ids=[
        "projects.delete",
        "sessions.revoke",
        "files.delete",
        "files.abort_upload",
        "context.delete",
        "domains.delete",
        "service_keys.revoke",
        "api_keys.revoke",
    ],
)
def test_204_delete_methods_retry_503_sync_and_async(monkeypatch, call_sync, call_async):
    monkeypatch.setattr("hot._transport.time.sleep", lambda _: None)

    async def fake_asleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("hot._transport.asyncio.sleep", fake_asleep)

    sync_attempts = 0

    def sync_handler(request: httpx.Request) -> httpx.Response:
        nonlocal sync_attempts
        assert request.method == "DELETE"
        sync_attempts += 1
        if sync_attempts == 1:
            return httpx.Response(503, json={"error": {"code": "unavailable", "message": "retry"}})
        return httpx.Response(204)

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(sync_handler)),
    ) as hot:
        assert call_sync(hot) is None
    assert sync_attempts == 2

    async_attempts = 0

    def async_handler(request: httpx.Request) -> httpx.Response:
        nonlocal async_attempts
        assert request.method == "DELETE"
        async_attempts += 1
        if async_attempts == 1:
            return httpx.Response(503, json={"error": {"code": "unavailable", "message": "retry"}})
        return httpx.Response(204)

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(async_handler)),
        ) as ahot:
            assert await call_async(ahot) is None

    asyncio.run(scenario())
    assert async_attempts == 2


def test_retry_after_parsing_edge_cases():
    # Bool excluded (must NOT be treated as int 1 or 0).
    err_true = parse_api_error(429, '{"error":{"message":"x","retry_after":true}}', {})
    err_false = parse_api_error(429, '{"error":{"message":"x","retry_after":false}}', {})
    assert err_true.retry_after is None
    assert err_false.retry_after is None

    # Negative values rejected.
    err_neg_body = parse_api_error(429, '{"error":{"message":"x","retry_after":-5}}', {})
    err_neg_hdr = parse_api_error(429, "", {"Retry-After": "-3"})
    assert err_neg_body.retry_after is None
    assert err_neg_hdr.retry_after is None

    # Zero is accepted (not dropped as falsy!).
    err_zero_body = parse_api_error(429, '{"error":{"message":"x","retry_after":0}}', {})
    err_zero_hdr = parse_api_error(429, "", {"Retry-After": "0"})
    assert err_zero_body.retry_after == 0
    assert err_zero_hdr.retry_after == 0

    # RFC 9110 HTTP-date fallback (future date and past date).
    future = datetime.now(timezone.utc) + timedelta(seconds=10)
    err_future = parse_api_error(429, "", {"Retry-After": format_datetime(future, usegmt=True)})
    assert err_future.retry_after is not None and 8 <= err_future.retry_after <= 12

    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    err_past = parse_api_error(429, "", {"RETRY-AFTER": format_datetime(past, usegmt=True)})
    assert err_past.retry_after == 0

    # Unparseable header -> None.
    err_bad = parse_api_error(429, "", {"Retry-After": "not-a-date"})
    assert err_bad.retry_after is None


def test_retry_after_zero_retries_immediately_and_negative_does_not(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("hot._transport.time.sleep", sleeps.append)

    calls = 0

    def handler_zero(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"data": {"id": "r1"}, "meta": {}})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler_zero)),
    ) as hot:
        assert hot.runs.get("r1") == {"id": "r1"}
    assert calls == 2
    assert sleeps == [0.0]

    neg_calls = 0

    def handler_neg(_request: httpx.Request) -> httpx.Response:
        nonlocal neg_calls
        neg_calls += 1
        return httpx.Response(429, headers={"Retry-After": "-5"})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler_neg)),
    ) as hot:
        with pytest.raises(HotApiError) as err:
            hot.runs.get("r1")
    assert err.value.status_code == 429
    assert neg_calls == 1


def test_retry_sleep_bounded_by_configured_timeout_sync_and_async(monkeypatch):
    sleeps: list[float] = []

    async def fake_asleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("hot._transport.time.sleep", sleeps.append)
    monkeypatch.setattr("hot._transport.asyncio.sleep", fake_asleep)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "20"})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(timeout=1.0, transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(HotApiError) as err:
            hot.runs.get("r1")
    assert err.value.status_code == 429
    assert sleeps == []

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(timeout=1.0, transport=httpx.MockTransport(handler)),
        ) as ahot:
            with pytest.raises(HotApiError) as aerr:
                await ahot.runs.get("r1")
            assert aerr.value.status_code == 429

    asyncio.run(scenario())
    assert sleeps == []


def test_base_url_normalization_and_case_insensitive_header_merge():
    for raw, expected in [
        ("https://api.hot.dev", "https://api.hot.dev"),
        ("https://api.hot.dev/", "https://api.hot.dev"),
        ("https://api.hot.dev///", "https://api.hot.dev"),
        ("https://api.hot.dev/v1", "https://api.hot.dev"),
        ("https://api.hot.dev/v1/", "https://api.hot.dev"),
        ("https://api.hot.dev/v1///", "https://api.hot.dev"),
        ("https://api.hot.dev/v10/", "https://api.hot.dev/v10"),
    ]:
        transport = SyncTransport(token="t", base_url=raw)
        try:
            assert transport.base_url == expected
            assert transport.api_base_url == f"{expected}/v1"
        finally:
            transport.close()

    transport = SyncTransport(token="t")
    try:
        merged = transport.headers({"user-agent": "custom/1.0", "accept": "application/json"})
        assert [k.lower() for k in merged].count("user-agent") == 1
        assert merged["user-agent"] == "custom/1.0"
        assert "User-Agent" not in merged
        assert merged["Authorization"] == "Bearer t"

        public = transport.headers({"Accept": "application/json"}, authenticated=False)
        assert "Authorization" not in public
        assert "User-Agent" in public
    finally:
        transport.close()


def test_non_json_2xx_and_missing_data_raise_structured_hot_api_error():
    def html_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>Proxy page</html>")

    def no_data_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": True})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(html_handler)),
    ) as hot:
        with pytest.raises(HotApiError) as html_err:
            hot.runs.get("r1")
        assert html_err.value.status_code == 200
        assert html_err.value.code == "invalid_json"

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(no_data_handler)),
    ) as hot:
        with pytest.raises(HotApiError) as missing_err:
            hot.runs.get("r1")
        assert missing_err.value.code == "invalid_response"

        with pytest.raises(HotApiError) as upload_err:
            hot.files.upload("a/b.txt", b"hi")
        assert upload_err.value.code == "invalid_response"

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(html_handler)),
        ) as ahot:
            with pytest.raises(HotApiError) as aerr:
                await ahot.runs.get("r1")
            assert aerr.value.code == "invalid_json"

    asyncio.run(scenario())


def test_large_non_json_error_body_is_truncated_and_none_status_handled():
    huge = "x" * 20_000
    err = parse_api_error(502, huge, {})
    assert len(err.message) < 5_000
    assert err.message.endswith("... (truncated)")

    task_err = HotTaskError({"task_id": "t1", "status": None, "result": None})
    assert str(task_err) == "Task failed"

    run_err = HotRunError({"run_id": "r1", "status": None, "result": None})
    assert str(run_err) == "Run failed"
    assert isinstance(run_err, RuntimeError)


def test_watchdog_cancel_and_rearm_prevent_stale_shutdown():
    shutdown_calls = 0

    class FakeSocket:
        def shutdown(self, _how: int) -> None:
            nonlocal shutdown_calls
            shutdown_calls += 1

    class FakeNetworkStream:
        def get_extra_info(self, key: str):
            return FakeSocket() if key == "socket" else None

    response = httpx.Response(200, extensions={"network_stream": FakeNetworkStream()})
    watchdog = _streaming._Watchdog(response, target=time.monotonic() + 60.0)
    gen1 = watchdog._generation

    # Rearm invalidates gen1; invoking stale _fire(gen1) must be a no-op.
    watchdog.rearm(time.monotonic() + 120.0)
    watchdog._fire(gen1)
    assert not watchdog.fired
    assert shutdown_calls == 0

    # Cancel invalidates current generation; invoking _fire after cancel must be a no-op.
    gen2 = watchdog._generation
    watchdog.cancel()
    watchdog._fire(gen2)
    assert not watchdog.fired
    assert shutdown_calls == 0


def test_aiter_stream_event_loop_timeout_fires_on_deadline_and_wake():
    class SlowAsyncStream(httpx.AsyncByteStream):
        def __init__(self, first: bytes | None = None) -> None:
            self.first = first
            self.closed = False

        async def __aiter__(self):
            if self.first is not None:
                yield self.first
            while True:
                await asyncio.sleep(10.0)
                yield b": keepalive\n\n"

        async def aclose(self) -> None:
            self.closed = True

    # 1) Deadline interrupts blocked read on the event loop and closes stream.
    stream1 = SlowAsyncStream()

    async def deadline_scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200, stream=stream1, headers={"content-type": "text/event-stream"}
                    )
                )
            ),
        ) as ahot:
            with pytest.raises(_streaming.StreamDeadline):
                async for _ in _streaming.aiter_stream(
                    ahot._transport,
                    "GET",
                    "/streams/s1/subscribe",
                    deadline=time.monotonic() + 0.15,
                ):
                    pass

    started = time.monotonic()
    asyncio.run(deadline_scenario())
    assert 0.1 <= time.monotonic() - started < 1.5
    assert stream1.closed

    # 2) Dynamic wake_at interrupts blocked read before a much later deadline.
    stream2 = SlowAsyncStream(b'data: {"type":"run:start","run":{"run_id":"r1"}}\n\n')

    async def wake_scenario():
        wake_time: list[float | None] = [None]
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200, stream=stream2, headers={"content-type": "text/event-stream"}
                    )
                )
            ),
        ) as ahot:
            with pytest.raises(_streaming.StreamWake):
                async for _event in _streaming.aiter_stream(
                    ahot._transport,
                    "GET",
                    "/streams/s1/subscribe",
                    deadline=time.monotonic() + 30.0,
                    wake=lambda: wake_time[0],
                ):
                    wake_time[0] = time.monotonic() + 0.15

    started = time.monotonic()
    asyncio.run(wake_scenario())
    assert 0.1 <= time.monotonic() - started < 1.5
    assert stream2.closed
