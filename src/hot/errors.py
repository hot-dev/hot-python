from __future__ import annotations

from typing import Any


class HotApiError(Exception):
    """Structured error raised for non-2xx Hot API responses."""

    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        code: str | None = None,
        request_id: str | None = None,
        headers: dict[str, str] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.code = code
        self.request_id = request_id
        self.headers = headers or {}
        self.error = error


def parse_api_error(status_code: int, text: str, headers: dict[str, str]) -> HotApiError:
    try:
        import json

        parsed = json.loads(text) if text else {}
    except ValueError:
        parsed = {}

    error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(error, dict):
        message = str(error.get("message") or f"Hot API error ({status_code})")
        code = str(error["code"]) if error.get("code") is not None else None
        request_id = str(error["request_id"]) if error.get("request_id") is not None else None
        return HotApiError(
            status_code=status_code,
            message=message,
            code=code,
            request_id=request_id,
            headers=headers,
            error=error,
        )

    return HotApiError(
        status_code=status_code,
        message=text or f"Hot API error ({status_code})",
        headers=headers,
    )
