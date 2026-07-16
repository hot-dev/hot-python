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
