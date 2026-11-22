from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, TypeAlias

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]
ApiResponse: TypeAlias = dict[str, Any]
ApiListResponse: TypeAlias = dict[str, Any]
StreamEvent: TypeAlias = dict[str, Any]
StreamEvents: TypeAlias = Iterator[StreamEvent]
AsyncStreamEvents: TypeAlias = AsyncIterator[StreamEvent]

__all__ = [
    "ApiListResponse",
    "ApiResponse",
    "AsyncStreamEvents",
    "JsonObject",
    "JsonValue",
    "StreamEvent",
    "StreamEvents",
]
