from __future__ import annotations

import asyncio
import importlib.metadata
import random
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import httpx

from .errors import HotApiError, parse_api_error

DEFAULT_BASE_URL = "https://api.hot.dev"

try:
    _VERSION = importlib.metadata.version("hot-dev")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    _VERSION = "unknown"

USER_AGENT = f"hot-sdk-python/{_VERSION}"

# Used when the caller passes neither ``timeout`` nor ``client``. Streams set
# their own timeouts (see resources.iter_stream). Pass ``httpx.Timeout(None)``
# to disable timeouts.
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# JSON requests retry on 429 when the server supplies retry_after, and
# idempotent requests also retry 502/503/504.
_MAX_RETRIES = 2
_MAX_RETRY_AFTER_SECONDS = 30.0
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE"})


def _retry_delay(method: str, response: httpx.Response, attempts: int) -> float | None:
    if attempts >= _MAX_RETRIES:
        return None
    status = response.status_code
    if status == 429:
        error = _error_of(response)
        if not error.retry_after:
            return None
        return min(float(error.retry_after), _MAX_RETRY_AFTER_SECONDS)
    if status in (502, 503, 504) and method.upper() in _IDEMPOTENT_METHODS:
        error = _error_of(response)
        if error.retry_after:
            return min(float(error.retry_after), _MAX_RETRY_AFTER_SECONDS)
        return 0.5 * 2**attempts * (0.5 + random.random() / 2)
    return None


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
        self.base_url = base_url.rstrip("/")
        self.api_base_url = f"{self.base_url}/v1"
        self.client = client or httpx.Client(timeout=_default_timeout(timeout))
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        attempts = 0
        while True:
            response = self.client.request(
                method,
                f"{self.api_base_url}{path}",
                headers=self.headers({"Accept": "application/json"}),
                json=json,
                params=without_none(params),
            )
            delay = _retry_delay(method, response, attempts)
            if delay is None:
                return parse_response(response)
            attempts += 1
            time.sleep(delay)

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
            f"{self.api_base_url}{path}",
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
            f"{self.api_base_url}{path}",
            headers=self.headers(),
            params=without_none(params),
        ) as response:
            if response.is_error:
                response.read()
            raise_for_status(response)
            yield response

    def headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": USER_AGENT,
            **dict(extra or {}),
        }


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
        self.base_url = base_url.rstrip("/")
        self.api_base_url = f"{self.base_url}/v1"
        self.client = client or httpx.AsyncClient(timeout=_default_timeout(timeout))
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        attempts = 0
        while True:
            response = await self.client.request(
                method,
                f"{self.api_base_url}{path}",
                headers=self.headers({"Accept": "application/json"}),
                json=json,
                params=without_none(params),
            )
            delay = _retry_delay(method, response, attempts)
            if delay is None:
                return parse_response(response)
            attempts += 1
            await asyncio.sleep(delay)

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
            f"{self.api_base_url}{path}",
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
            f"{self.api_base_url}{path}",
            headers=self.headers(),
            params=without_none(params),
        ) as response:
            if response.is_error:
                await response.aread()
            raise_for_status(response)
            yield response

    def headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": USER_AGENT,
            **dict(extra or {}),
        }


def parse_response(response: httpx.Response) -> dict[str, Any]:
    raise_for_status(response)
    if not response.content:
        return {"data": None, "meta": {}}
    parsed = response.json()
    if not isinstance(parsed, dict):
        return {"data": parsed, "meta": {}}
    return parsed


def raise_for_status(response: httpx.Response) -> None:
    if response.is_error:
        raise _error_of(response)


def without_none(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}
