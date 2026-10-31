"""Integration tests against a live ``hot dev`` instance.

The server must serve the fixture project in integration/fixture. Start one
and run this file with integration/run.sh; without HOT_TEST_API_KEY set, the
whole module is skipped.
"""

from __future__ import annotations

import os

import pytest

from hot import HotClient

pytestmark = pytest.mark.skipif(
    not os.environ.get("HOT_TEST_API_KEY"),
    reason="HOT_TEST_API_KEY not set; run integration/run.sh",
)


@pytest.fixture
def hot():
    client = HotClient(
        token=os.environ["HOT_TEST_API_KEY"],
        base_url=os.environ.get("HOT_TEST_BASE_URL", "http://localhost:4681"),
    )
    with client:
        yield client


def test_env_and_projects(hot: HotClient) -> None:
    env = hot.env.get()
    assert env["name"] == "development"

    projects = hot.projects.list()
    names = [project["name"] for project in projects["data"]]
    assert "sdk-fixture" in names


def test_subscribe_with_event_echo(hot: HotClient) -> None:
    result = None
    for event in hot.streams.subscribe_with_event(
        {"event_type": "sdk:echo", "event_data": {"text": "hello"}}
    ):
        if event["type"] in ("run:fail", "run:cancel"):
            pytest.fail(f"run did not complete: {event}")
        if event["type"] == "run:stop":
            result = event["run"]["result"]
            break

    assert result == {"echoed": {"text": "hello"}}


def test_wait_for_task_started_by_event_handler(hot: HotClient) -> None:
    task_id = None
    handler_run_id = None
    stream_id = None
    for event in hot.streams.subscribe_with_event(
        {"event_type": "sdk:task", "event_data": {"value": "hello"}}
    ):
        if event["type"] in ("run:fail", "run:cancel"):
            pytest.fail(f"run did not complete: {event}")
        if event["type"] == "run:stop":
            task_id = event["run"]["result"]["work"]["task"]
            handler_run_id = event["run"]["run_id"]
            stream_id = event["run"]["stream_id"]
            break

    assert isinstance(task_id, str)
    assert hot.runs.wait(handler_run_id, timeout=60)["status"] == "succeeded"

    streamed_task = None
    for event in hot.streams.subscribe(stream_id):
        if event.get("type") != "task:update":
            continue
        task = event.get("task")
        if not isinstance(task, dict) or task.get("task_id") != task_id:
            continue
        if task.get("status") in ("failed", "cancelled", "timed_out"):
            pytest.fail(f"task did not complete: {task}")
        if task.get("status") == "completed":
            streamed_task = task
            break

    assert streamed_task is not None
    assert streamed_task["result"] == {"processed": "hello"}
    task = hot.tasks.wait(task_id, timeout=60)
    assert task["status"] == "completed"
    assert task["result"] == {"processed": "hello"}


def test_call_hot_add_nums(hot: HotClient) -> None:
    assert hot.events.call_hot("::fixture::sdk/add-nums", [2, 3]) == 5


def test_wait_for_run_result_times_out_on_silent_stream(hot: HotClient) -> None:
    import time

    # An event without a handler produces a stream that stays silent.
    published = hot.events.publish({"event_type": "sdk:no-handler", "event_data": {}})

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        hot.streams.wait_for_run_result(
            published["stream_id"],
            published["event_id"],
            timeout=1.5,
        )
    assert time.monotonic() - started < 10


def test_run_failure_message(hot: HotClient) -> None:
    published = hot.events.publish({"event_type": "sdk:fail", "event_data": {}})

    with pytest.raises(RuntimeError, match="sdk fixture failure"):
        hot.streams.wait_for_run_result(
            published["stream_id"],
            published["event_id"],
        )


def test_call_hot_on_chunk_receives_chunks_sent_before_run_start(hot: HotClient) -> None:
    # Hot pushes stream:data (and run:stop) before run:start for short runs.
    chunks: list[str] = []
    result = hot.events.call_hot("::fixture::sdk/stream-chunks", on_chunk=chunks.append)
    assert result == "done"
    assert chunks == ["a", "b", "c"]


def test_async_call_hot_on_chunk() -> None:
    import asyncio

    from hot import AsyncHotClient

    async def run() -> tuple[object, list[str]]:
        chunks: list[str] = []
        async with AsyncHotClient(
            token=os.environ["HOT_TEST_API_KEY"],
            base_url=os.environ.get("HOT_TEST_BASE_URL", "http://localhost:4681"),
        ) as client:
            result = await client.events.call_hot(
                "::fixture::sdk/stream-chunks", on_chunk=chunks.append
            )
        return result, chunks

    assert asyncio.run(run()) == ("done", ["a", "b", "c"])


def test_waiters_follow_a_retried_handler(hot: HotClient) -> None:
    # sdk:flaky fails its first attempt (run:fail, status pending_retry); the
    # retry run has a new event_id and origin_run_id pointing at the failure.
    published = hot.events.publish({"event_type": "sdk:flaky", "event_data": {}})
    run = hot.streams.wait_for_run_result(published["stream_id"], published["event_id"], timeout=60)
    assert run["result"] == {"recovered": True, "attempt": 1}
    assert run["origin_run_id"] is not None

    events = list(hot.streams.subscribe_with_event({"event_type": "sdk:flaky", "event_data": {}}))
    assert [event["run"]["status"] for event in events if event["type"] == "run:fail"] == [
        "pending_retry"
    ]
    assert events[-1]["type"] == "run:stop"
    assert events[-1]["run"]["result"] == {"recovered": True, "attempt": 1}


def test_file_download_stream(hot: HotClient) -> None:
    content = os.urandom(256 * 1024)
    uploaded = hot.files.upload("sdk-integration/download.bin", content)
    try:
        with hot.files.download_stream(uploaded["file_id"]) as response:
            assert b"".join(response.iter_bytes()) == content
        assert hot.files.download(uploaded["file_id"]).content == content
    finally:
        hot.files.delete(uploaded["file_id"])
