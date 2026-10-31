from __future__ import annotations

import re
from typing import Any, Literal

KeyScope = Literal["org", "env"]

_ORG_ROUTE = re.compile(r"/v1/(?:orgs|api-keys)(?:/|$)")


class HotApiError(Exception):
    """Structured error raised for non-2xx Hot API responses."""

    def __init__(
        self,
        *,
        status_code: int,
        message: str,
        code: str | None = None,
        request_id: str | None = None,
        retry_after: int | None = None,
        headers: dict[str, str] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.code = code
        self.request_id = request_id
        self.retry_after = retry_after
        self.headers = headers or {}
        self.error = error


class HotKeyScopeError(HotApiError):
    """Raised for ``403 key_scope_mismatch``: the credential is the wrong kind of key.

    Org API keys only call ``/v1/orgs`` and ``/v1/api-keys``; environment API
    keys, service keys and sessions only call runtime routes.
    ``required_scope`` names the kind of key the call needs: ``"org"`` or ``"env"``.
    """

    def __init__(self, *, required_scope: KeyScope, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.required_scope: KeyScope = required_scope


class HotTaskError(Exception):
    """Raised when a waited task reaches a non-success terminal state."""

    def __init__(self, task: dict[str, Any]) -> None:
        result = task.get("result")
        message = _result_message(result) or f"Task {task.get('status', 'failed')}"
        super().__init__(message)
        self.task = task


class HotRunError(Exception):
    """Raised when a waited run reaches a non-success terminal state."""

    def __init__(self, run: dict[str, Any]) -> None:
        result = run.get("result")
        status = run.get("status", "failed")
        message = _result_message(result) or f"Run {status}"
        super().__init__(message)
        self.run = run


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


def parse_api_error(
    status_code: int,
    text: str,
    headers: dict[str, str],
    path: str | None = None,
) -> HotApiError:
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
        retry_after = _retry_after(error.get("retry_after"), headers)
        if status_code == 403 and code == "key_scope_mismatch":
            scope = _required_scope(path, message)
            return HotKeyScopeError(
                required_scope=scope,
                status_code=status_code,
                message=_SCOPE_MESSAGES[scope],
                code=code,
                request_id=request_id,
                retry_after=retry_after,
                headers=headers,
                error=error,
            )
        return HotApiError(
            status_code=status_code,
            message=message,
            code=code,
            request_id=request_id,
            retry_after=retry_after,
            headers=headers,
            error=error,
        )

    return HotApiError(
        status_code=status_code,
        message=text or f"Hot API error ({status_code})",
        retry_after=_retry_after(None, headers),
        headers=headers,
    )


_SCOPE_MESSAGES: dict[KeyScope, str] = {
    "org": (
        "This call needs an org API key; environment API keys, service keys "
        "and sessions only call runtime routes"
    ),
    "env": (
        "This call needs an environment API key; org API keys only call /v1/orgs and /v1/api-keys"
    ),
}


def _required_scope(path: str | None, message: str) -> KeyScope:
    if path is not None:
        return "org" if _ORG_ROUTE.search(path) else "env"
    return "org" if "needs an org API key" in message else "env"


def _retry_after(value: Any, headers: dict[str, str]) -> int | None:
    if isinstance(value, int):
        return value
    header_value = headers.get("retry-after") or headers.get("Retry-After")
    if not header_value:
        return None
    try:
        return int(header_value)
    except ValueError:
        return None
