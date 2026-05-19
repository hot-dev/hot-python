from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from .types import StreamEvent


def consume_sse_blocks(buffer: str) -> tuple[list[StreamEvent], str]:
    events: list[StreamEvent] = []
    remaining = buffer

    while "\n\n" in remaining:
        block, remaining = remaining.split("\n\n", 1)
        data_payload = ""
        has_data = False

        for line in block.splitlines():
            if line.startswith("data: "):
                data_payload += line[6:]
                has_data = True
            elif line.startswith("data:"):
                data_payload += line[5:]
                has_data = True

        if not has_data:
            continue

        try:
            parsed: Any = json.loads(data_payload)
        except ValueError:
            continue

        if isinstance(parsed, dict) and isinstance(parsed.get("type"), str):
            events.append(parsed)

    return events, remaining


def iter_sse_events(lines: Iterable[str]) -> Iterator[StreamEvent]:
    buffer = ""
    for line in lines:
        buffer += line
        if not line.endswith("\n"):
            buffer += "\n"
        events, buffer = consume_sse_blocks(buffer)
        yield from events

    events, _ = consume_sse_blocks(f"{buffer}\n\n")
    yield from events
