from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from typing import Any

from .types import StreamEvent

# Maximum buffered SSE payload size before raising to prevent unbounded memory growth.
MAX_SSE_BUFFER_SIZE = 16 * 1024 * 1024

# WHATWG SSE §9.2.6 line endings: CRLF, LF, or CR only (never Unicode U+2028/U+2029).
# Use \r(?!\n) so a single CRLF can never backtrack into CR + LF.
_EVENT_DELIMITER = re.compile(r"(?:\r\n|\n|\r(?!\n))(?:\r\n|\n|\r(?!\n))")
_LINE_SPLIT = re.compile(r"\r\n|\n|\r")


def _parse_sse_block(block: str) -> StreamEvent | None:
    if not block:
        return None

    data_lines: list[str] = []
    for line in _LINE_SPLIT.split(block):
        if not line or line.startswith(":"):
            continue
        if ":" in line:
            field, value = line.split(":", 1)
            if value.startswith(" "):
                value = value[1:]
        else:
            field, value = line, ""
        if field == "data":
            data_lines.append(value)

    if not data_lines:
        return None

    try:
        parsed: Any = json.loads("\n".join(data_lines))
    except ValueError:
        return None

    if isinstance(parsed, dict) and isinstance(parsed.get("type"), str):
        return parsed
    return None


def consume_sse_blocks(
    buffer: str,
    *,
    max_buffer_size: int = MAX_SSE_BUFFER_SIZE,
    flush: bool = False,
) -> tuple[list[StreamEvent], str]:
    if len(buffer) > max_buffer_size:
        raise ValueError(f"SSE buffer exceeded maximum size of {max_buffer_size} characters")

    if buffer.startswith("\ufeff"):
        buffer = buffer[1:]

    events: list[StreamEvent] = []
    pos = 0
    for match in _EVENT_DELIMITER.finditer(buffer):
        block = buffer[pos : match.start()]
        pos = match.end()
        event = _parse_sse_block(block)
        if event is not None:
            events.append(event)

    remaining = buffer[pos:]
    if flush and remaining:
        event = _parse_sse_block(remaining)
        if event is not None:
            events.append(event)
        remaining = ""

    return events, remaining


def iter_sse_events(
    lines: Iterable[str],
    *,
    max_buffer_size: int = MAX_SSE_BUFFER_SIZE,
) -> Iterator[StreamEvent]:
    buffer = ""
    for line in lines:
        buffer += line
        if not line.endswith(("\n", "\r")):
            buffer += "\n"
        events, buffer = consume_sse_blocks(buffer, max_buffer_size=max_buffer_size)
        yield from events

    if buffer:
        events, _ = consume_sse_blocks(buffer, max_buffer_size=max_buffer_size, flush=True)
        yield from events
