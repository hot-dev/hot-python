from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from hot import AsyncHotClient, HotClient, consume_sse_blocks
from hot.sse import MAX_SSE_BUFFER_SIZE, iter_sse_events


def test_unicode_line_separators_do_not_split_data_lines():
    payload = {"type": "stream:data", "text": "a\u2028b\u2029c\x85d\x0be\x0cf"}
    raw_json = json.dumps(payload, ensure_ascii=False)
    assert "\u2028" in raw_json and "\u2029" in raw_json

    events, remaining = consume_sse_blocks(f"data: {raw_json}\n\n")
    assert events == [payload]
    assert remaining == ""


@pytest.mark.parametrize("delim", ["\n\n", "\r\n\r\n", "\r\r", "\r\n\n", "\n\r\n"])
def test_event_delimiters_lf_crlf_cr(delim: str):
    stream = (
        f'data: {{"type":"a","n":1}}{delim}'
        f'data: {{"type":"b","n":2}}{delim}'
        f'data: {{"type":"partial"'
    )
    events, remaining = consume_sse_blocks(stream)
    assert events == [{"type": "a", "n": 1}, {"type": "b", "n": 2}]
    assert remaining == 'data: {"type":"partial"'


def test_multiline_data_lines_joined_with_newline(monkeypatch):
    # Pretty-printed JSON across multiple data: lines with CRLF/CR/LF.
    block = 'data: {\r\ndata:   "type": "run:stop",\rdata:   "ok": true\ndata: }\r\n\r\n'
    seen_payloads: list[str] = []
    real_loads = json.loads

    def spy_loads(s: str, *args, **kwargs):
        seen_payloads.append(s)
        return real_loads(s, *args, **kwargs)

    monkeypatch.setattr("hot.sse.json.loads", spy_loads)
    events, remaining = consume_sse_blocks(block)
    assert events == [{"type": "run:stop", "ok": True}]
    assert remaining == ""
    assert seen_payloads == ['{\n  "type": "run:stop",\n  "ok": true\n}']


def test_multiline_data_not_concatenated_without_newline():
    # If joined with "", "1" + "2" would become valid JSON {"type": "x", "n": 12}.
    # Per WHATWG SSE §9.2.6, lines are joined with "\n", making "1\n2" invalid JSON.
    events, remaining = consume_sse_blocks('data: {"type": "x", "n": 1\ndata: 2}\n\n')
    assert events == []
    assert remaining == ""


def test_data_field_spacing_comments_and_other_fields():
    stream = (
        ": keepalive comment\n\n"
        "event: message\n"
        "id: 42\n"
        "retry: 3000\n"
        ": inline comment\n"
        'data:  {"type":"spaced"}\n\n'
        'data:{"type":"nospace"}\n\n'
        "data\n"
        'data: {"type":"after_bare_data"}\n\n'
        "data: not-json\n\n"
        "data: [1, 2, 3]\n\n"
        'data: {"missing_type": true}\n\n'
    )
    events, remaining = consume_sse_blocks(stream)
    assert events == [
        {"type": "spaced"},
        {"type": "nospace"},
        {"type": "after_bare_data"},
    ]
    assert remaining == ""


def test_leading_utf8_bom_is_stripped():
    events, remaining = consume_sse_blocks('\ufeffdata: {"type":"bom"}\n\n')
    assert events == [{"type": "bom"}]
    assert remaining == ""

    # BOM arriving alone in the first chunk.
    events1, buf = consume_sse_blocks("\ufeff")
    assert events1 == []
    assert buf == ""
    events2, buf = consume_sse_blocks(buf + 'data: {"type":"after_bom"}\n\n')
    assert events2 == [{"type": "after_bom"}]
    assert buf == ""


def test_chunk_boundaries_including_split_crlf():
    chunks = [
        "\ufeff",
        "data: {",
        '"type":"chunked",',
        '"text":"hi\u2028there"}\r',
        "\n\r",
        "\n",
        ": keep",
        "alive\r\n\r\n",
        'data: {"type":"second"}\r',
        "\r",
        'data: {"type":"eof_only"}',
    ]
    buf = ""
    events = []
    for chunk in chunks:
        buf += chunk
        parsed, buf = consume_sse_blocks(buf)
        events.extend(parsed)
    parsed, buf = consume_sse_blocks(buf, flush=True)
    events.extend(parsed)
    assert buf == ""
    assert events == [
        {"type": "chunked", "text": "hi\u2028there"},
        {"type": "second"},
        {"type": "eof_only"},
    ]


def test_iter_sse_events_flushes_and_handles_cr():
    lines = [
        'data: {"type":"one"}\r',
        "\r",
        'data: {"type":"two"}',
    ]
    assert list(iter_sse_events(lines)) == [{"type": "one"}, {"type": "two"}]


def test_max_buffer_cap_raises_clean_error():
    oversized = "data: " + ("x" * 100)
    with pytest.raises(ValueError, match="SSE buffer exceeded"):
        consume_sse_blocks(oversized, max_buffer_size=64)

    # Exact cap works cleanly, including flush at EOF.
    exact = 'data: {"type":"ok"}'
    events, remaining = consume_sse_blocks(exact, max_buffer_size=len(exact), flush=True)
    assert events == [{"type": "ok"}]
    assert remaining == ""


def test_stream_enforces_max_buffer_cap_sync_and_async():
    payload = b"data: " + (b"x" * (MAX_SSE_BUFFER_SIZE + 1))

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, headers={"content-type": "text/event-stream"})

    with HotClient(
        token="t",
        base_url="https://api.example.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as hot:
        with pytest.raises(ValueError, match="SSE buffer exceeded"):
            list(hot.streams.subscribe("s1"))

    async def scenario():
        async with AsyncHotClient(
            token="t",
            base_url="https://api.example.test",
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ) as ahot:
            with pytest.raises(ValueError, match="SSE buffer exceeded"):
                [event async for event in ahot.streams.subscribe("s1")]

    asyncio.run(scenario())


def test_many_events_parsed_linearly():
    count = 2000
    stream = "".join(f'data: {{"type":"tick","i":{i}}}\r\n\r\n' for i in range(count))
    events, remaining = consume_sse_blocks(stream)
    assert len(events) == count
    assert events[0] == {"type": "tick", "i": 0}
    assert events[-1] == {"type": "tick", "i": count - 1}
    assert remaining == ""
