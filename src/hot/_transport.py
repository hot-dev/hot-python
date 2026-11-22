from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import httpx

from ._version import __version__
from .errors import HotApiError, parse_api_error, truncate_error_text

DEFAULT_BASE_URL = "https://api.hot.dev"
USER_AGENT = f"hot-sdk-python/{__version__}"

# Used when the caller passes neither ``timeout`` nor ``client``. Streams set
# their own timeouts (see _streaming.iter_stream). Pass ``httpx.Timeout(None)``
# to disable timeouts.
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# JSON requests retry on 429 when the server supplies retry_after, idempotent
# requests also retry 502/503/504, and POST retries 503 event_publish_in_progress.
_MAX_RETRIES = 2
_MAX_RETRY_AFTER_SECONDS = 30.0
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE"})


def _normalize_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/v1"):
        normalized = normalized[:-3].rstrip("/")
    return normalized


def _normalize_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"


def _merge_headers(
    base: Mapping[str, str],
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    merged = dict(base)
    if not extra:
        return merged
    by_lower = {key.lower(): key for key in merged}
    for key, value in extra.items():
        existing = by_lower.get(key.lower())
        if existing is not None and existing != key:
            del merged[existing]
        merged[key] = value
        by_lower[key.lower()] = key
    return merged


def _retry_after_sleep(retry_after: int) -> float:
    base = min(float(retry_after), _MAX_RETRY_AFTER_SECONDS)
    if base <= 0.0:
        return 0.0
    jitter = random.uniform(0.0, min(1.0, base * 0.25))
    return min(base + jitter, _MAX_RETRY_AFTER_SECONDS)


def _backoff_or_retry_after(error: HotApiError, attempts: int) -> float:
    if error.retry_after is not None:
        return _retry_after_sleep(error.retry_after)
    return 0.5 * 2**attempts * (0.5 + random.random() / 2)


def _retry_delay(method: str, response: httpx.Response, attempts: int) -> float | None:
    if attempts >= _MAX_RETRIES:
        return None
    status = response.status_code
    if status == 429:
        error = _error_of(response)
        if error.retry_after is None:
            return None
        return _retry_after_sleep(error.retry_after)
    upper_method = method.upper()
    if status in (502, 503, 504) and upper_method in _IDEMPOTENT_METHODS:
        return _backoff_or_retry_after(_error_of(response), attempts)
    if status == 503 and upper_method == "POST":
        error = _error_of(response)
        if error.code == "event_publish_in_progress":
            return _backoff_or_retry_after(error, attempts)
    return None


def _retry_deadline(timeout: httpx.Timeout | None) -> float | None:
    if timeout is None or timeout.read is None:
        return None
    return time.monotonic() + timeout.read


def _error_of(response: httpx.Response) -> HotApiError:
    return parse_api_error(
        response.status_code,
        response.text,
        dict(response.headers),
        _request_path(response),
    )


def _request_path(response: httpx.Response) -> str | None:
    try:
        return response.request.url.path
    except RuntimeError:
        return None


def _default_timeout(timeout: float | httpx.Timeout | None) -> float | httpx.Timeout:
    return DEFAULT_TIMEOUT if timeout is None else timeout


class SyncTransport:
    def __init__(
        self,
        *,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float | httpx.Timeout | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.token = token
        self.base_url = _normalize_base_url(base_url)
        self.api_base_url = f"{self.base_url}/v1"
        self.client = client or httpx.Client(timeout=_default_timeout(timeout))
        self._owns_client = client is None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, token='***')"

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def _url(self, path: str, *, versioned: bool = True) -> str:
        prefix = self.api_base_url if versioned else self.base_url
        return f"{prefix}{_normalize_path(path)}"

    def _request_json_response(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
        versioned: bool = True,
    ) -> httpx.Response:
        attempts = 0
        deadline = _retry_deadline(self.client.timeout)
        url = self._url(path, versioned=versioned)
        req_headers = self.headers({"Accept": "application/json"}, authenticated=authenticated)
        req_params = without_none(params)
        while True:
            response = self.client.request(
                method,
                url,
                headers=req_headers,
                json=json,
                params=req_params,
            )
            delay = _retry_delay(method, response, attempts)
            if delay is None or (deadline is not None and time.monotonic() + delay > deadline):
                return response
            attempts += 1
            time.sleep(delay)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
        versioned: bool = True,
    ) -> dict[str, Any]:
        response = self._request_json_response(
            method,
            path,
            json=json,
            params=params,
            authenticated=authenticated,
            versioned=versioned,
        )
        return parse_response(response)

    def request_data(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        response = self._request_json_response(method, path, json=json, params=params)
        return parse_data(response)

    def request_raw(
        self,
        method: str,
        path: str,
        *,
        content: Any = None,
        files: Any = None,
        data: Any = None,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        response = self.client.request(
            method,
            self._url(path),
            headers=self.headers(headers),
            content=content,
            files=files,
            data=data,
            params=without_none(params),
        )
        raise_for_status(response)
        return response

    @contextmanager
    def stream_raw(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[httpx.Response]:
        """Open a request without reading its body; the body streams on demand."""
        with self.client.stream(
            method,
            self._url(path),
            headers=self.headers(),
            params=without_none(params),
        ) as response:
            if not response.is_success:
                response.read()
            raise_for_status(response)
            yield response

    def headers(
        self,
        extra: Mapping[str, str] | None = None,
        *,
        authenticated: bool = True,
    ) -> dict[str, str]:
        base = {"User-Agent": USER_AGENT}
        if authenticated:
            base["Authorization"] = f"Bearer {self.token}"
        return _merge_headers(base, extra)


class AsyncTransport:
    def __init__(
        self,
        *,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float | httpx.Timeout | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.token = token
        self.base_url = _normalize_base_url(base_url)
        self.api_base_url = f"{self.base_url}/v1"
        self.client = client or httpx.AsyncClient(timeout=_default_timeout(timeout))
        self._owns_client = client is None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, token='***')"

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    def _url(self, path: str, *, versioned: bool = True) -> str:
        prefix = self.api_base_url if versioned else self.base_url
        return f"{prefix}{_normalize_path(path)}"

    async def _request_json_response(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
        versioned: bool = True,
    ) -> httpx.Response:
        attempts = 0
        deadline = _retry_deadline(self.client.timeout)
        url = self._url(path, versioned=versioned)
        req_headers = self.headers({"Accept": "application/json"}, authenticated=authenticated)
        req_params = without_none(params)
        while True:
            response = await self.client.request(
                method,
                url,
                headers=req_headers,
                json=json,
                params=req_params,
            )
            delay = _retry_delay(method, response, attempts)
            if delay is None or (deadline is not None and time.monotonic() + delay > deadline):
                return response
            attempts += 1
            await asyncio.sleep(delay)

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
        versioned: bool = True,
    ) -> dict[str, Any]:
        response = await self._request_json_response(
            method,
            path,
            json=json,
            params=params,
            authenticated=authenticated,
            versioned=versioned,
        )
        return parse_response(response)

    async def request_data(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        response = await self._request_json_response(method, path, json=json, params=params)
        return parse_data(response)

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        content: Any = None,
        files: Any = None,
        data: Any = None,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        response = await self.client.request(
            method,
            self._url(path),
            headers=self.headers(headers),
            content=content,
            files=files,
            data=data,
            params=without_none(params),
        )
        raise_for_status(response)
        return response

    @asynccontextmanager
    async def stream_raw(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """Open a request without reading its body; the body streams on demand."""
        async with self.client.stream(
            method,
            self._url(path),
            headers=self.headers(),
            params=without_none(params),
        ) as response:
            if not response.is_success:
                await response.aread()
            raise_for_status(response)
            yield response

    def headers(
        self,
        extra: Mapping[str, str] | None = None,
        *,
        authenticated: bool = True,
    ) -> dict[str, str]:
        base = {"User-Agent": USER_AGENT}
        if authenticated:
            base["Authorization"] = f"Bearer {self.token}"
        return _merge_headers(base, extra)


def parse_response(response: httpx.Response) -> dict[str, Any]:
    raise_for_status(response)
    if not response.content:
        return {"data": None, "meta": {}}
    try:
        parsed: Any = response.json()
    except ValueError as error:
        snippet = truncate_error_text(response.text) if response.text else ""
        detail = f": {snippet}" if snippet else ""
        raise HotApiError(
            status_code=response.status_code,
            message=f"Expected JSON response from Hot API ({response.status_code}){detail}",
            code="invalid_json",
            headers=dict(response.headers),
        ) from error
    if not isinstance(parsed, dict):
        return {"data": parsed, "meta": {}}
    return parsed


def parse_data(response: httpx.Response) -> Any:
    envelope = parse_response(response)
    if "data" not in envelope:
        raise HotApiError(
            status_code=response.status_code,
            message="Unexpected Hot API response: missing 'data' field",
            code="invalid_response",
            headers=dict(response.headers),
        )
    return envelope["data"]


def raise_for_status(response: httpx.Response) -> None:
    if not response.is_success:
        raise _error_of(response)


def without_none(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}
