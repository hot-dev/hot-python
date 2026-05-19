from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from .errors import parse_api_error

DEFAULT_BASE_URL = "https://api.hot.dev"


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
        self.client = client or httpx.Client(timeout=timeout)
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
        response = self.client.request(
            method,
            f"{self.api_base_url}{path}",
            headers=self.headers({"Accept": "application/json"}),
            json=json,
            params=without_none(params),
        )
        return parse_response(response)

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

    def headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}", **dict(extra or {})}


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
        self.client = client or httpx.AsyncClient(timeout=timeout)
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
        response = await self.client.request(
            method,
            f"{self.api_base_url}{path}",
            headers=self.headers({"Accept": "application/json"}),
            json=json,
            params=without_none(params),
        )
        return parse_response(response)

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

    def headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}", **dict(extra or {})}


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
        raise parse_api_error(response.status_code, response.text, dict(response.headers))


def without_none(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}
