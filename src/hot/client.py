from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self

import httpx

from ._transport import AsyncTransport, SyncTransport
from .resources import (
    AsyncEventsResource,
    AsyncStreamsResource,
    BuildsResource,
    ContextResource,
    DomainsResource,
    EnvResource,
    EventsResource,
    FilesResource,
    OrgResource,
    ProjectsResource,
    RunsResource,
    ServiceKeysResource,
    SessionsResource,
    StreamsResource,
)


class HotClient:
    """Synchronous Hot API client."""

    def __init__(
        self,
        *,
        token: str,
        base_url: str = "https://api.hot.dev",
        timeout: float | httpx.Timeout | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self._transport = SyncTransport(
            token=token,
            base_url=base_url,
            timeout=timeout,
            client=client,
        )
        self.builds = BuildsResource(self._transport)
        self.context = ContextResource(self._transport)
        self.domains = DomainsResource(self._transport)
        self.env = EnvResource(self._transport)
        self.events = EventsResource(self._transport)
        self.files = FilesResource(self._transport)
        self.org = OrgResource(self._transport)
        self.projects = ProjectsResource(self._transport)
        self.runs = RunsResource(self._transport)
        self.service_keys = ServiceKeysResource(self._transport)
        self.sessions = SessionsResource(self._transport)
        self.streams = StreamsResource(self._transport)

    @property
    def token(self) -> str:
        return self._transport.token

    @property
    def api_base_url(self) -> str:
        return self._transport.api_base_url

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call a JSON API endpoint that does not yet have a resource helper."""
        return self._transport.request_json(method, path, json=json, params=params)

    def request_raw(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Call an API endpoint and return the raw HTTP response."""
        return self._transport.request_raw(method, path, **kwargs)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncHotClient:
    """Asynchronous Hot API client.

    Async support currently focuses on event publishing and stream consumption,
    where async is most valuable.
    """

    def __init__(
        self,
        *,
        token: str,
        base_url: str = "https://api.hot.dev",
        timeout: float | httpx.Timeout | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._transport = AsyncTransport(
            token=token,
            base_url=base_url,
            timeout=timeout,
            client=client,
        )
        self.events = AsyncEventsResource(self._transport)
        self.streams = AsyncStreamsResource(self._transport)

    @property
    def token(self) -> str:
        return self._transport.token

    @property
    def api_base_url(self) -> str:
        return self._transport.api_base_url

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call a JSON API endpoint that does not yet have a resource helper."""
        return await self._transport.request_json(method, path, json=json, params=params)

    async def request_raw(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Call an API endpoint and return the raw HTTP response."""
        return await self._transport.request_raw(method, path, **kwargs)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()
