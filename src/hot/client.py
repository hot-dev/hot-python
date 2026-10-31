from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from ._transport import AsyncTransport, SyncTransport
from .resources import (
    ApiKeysResource,
    AsyncApiKeysResource,
    AsyncBlobsResource,
    AsyncBuildsResource,
    AsyncContextResource,
    AsyncDomainsResource,
    AsyncEnvResource,
    AsyncEventsResource,
    AsyncFilesResource,
    AsyncOrgResource,
    AsyncOrgsResource,
    AsyncProjectsResource,
    AsyncRunsResource,
    AsyncServiceKeysResource,
    AsyncSessionsResource,
    AsyncStreamsResource,
    AsyncTasksResource,
    BlobsResource,
    BuildsResource,
    ContextResource,
    DomainsResource,
    EnvResource,
    EventsResource,
    FilesResource,
    OrgResource,
    OrgsResource,
    ProjectsResource,
    RunsResource,
    ServiceKeysResource,
    SessionsResource,
    StreamsResource,
    TasksResource,
)


class HotClient:
    """Synchronous Hot API client.

    Pass an environment API key (or a service key or session) for runtime
    resources, or an org API key for ``orgs`` and ``api_keys``. A call with the
    other kind of key raises :class:`hot.HotKeyScopeError`. ``timeout``
    defaults to 60 s per read and 10 s to connect; pass ``httpx.Timeout(None)``
    to disable it. Streams and waiters set their own timeouts.
    """

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
        self.api_keys = ApiKeysResource(self._transport)
        self.blobs = BlobsResource(self._transport)
        self.builds = BuildsResource(self._transport)
        self.context = ContextResource(self._transport)
        self.domains = DomainsResource(self._transport)
        self.env = EnvResource(self._transport)
        self.events = EventsResource(self._transport)
        self.files = FilesResource(self._transport)
        self.org = OrgResource(self._transport)
        self.orgs = OrgsResource(self._transport)
        self.projects = ProjectsResource(self._transport)
        self.runs = RunsResource(self._transport)
        self.service_keys = ServiceKeysResource(self._transport)
        self.sessions = SessionsResource(self._transport)
        self.streams = StreamsResource(self._transport)
        self.tasks = TasksResource(self._transport)

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

    def __enter__(self) -> HotClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncHotClient:
    """Asynchronous Hot API client."""

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
        self.api_keys = AsyncApiKeysResource(self._transport)
        self.blobs = AsyncBlobsResource(self._transport)
        self.builds = AsyncBuildsResource(self._transport)
        self.context = AsyncContextResource(self._transport)
        self.domains = AsyncDomainsResource(self._transport)
        self.env = AsyncEnvResource(self._transport)
        self.events = AsyncEventsResource(self._transport)
        self.files = AsyncFilesResource(self._transport)
        self.org = AsyncOrgResource(self._transport)
        self.orgs = AsyncOrgsResource(self._transport)
        self.projects = AsyncProjectsResource(self._transport)
        self.runs = AsyncRunsResource(self._transport)
        self.service_keys = AsyncServiceKeysResource(self._transport)
        self.sessions = AsyncSessionsResource(self._transport)
        self.streams = AsyncStreamsResource(self._transport)
        self.tasks = AsyncTasksResource(self._transport)

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

    async def __aenter__(self) -> AsyncHotClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()
