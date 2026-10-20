from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from ._transport import AsyncTransport, SyncTransport
from .resources import enc


class HotManagementClient:
    """Root management API client. Requires an ``hm_`` token, issued by a root admin.

    Runtime API keys, service keys, and sessions cannot call these endpoints.
    Use a separate HotClient with a child environment key for runtime access.
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
            token=token, base_url=base_url, timeout=timeout, client=client
        )

    def list_children(self, *, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        """Return ``children`` and ``total`` for the credential's authorized targets."""
        envelope = self._transport.request_json(
            "GET", "/management/children", params={"limit": limit, "offset": offset}
        )
        return envelope["data"]

    def create_child(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Provision a child using a required, retry-stable ``idempotency_key``."""
        envelope = self._transport.request_json("POST", "/management/children", json=dict(body))
        return envelope["data"]

    def set_child_state(self, child_id: str, state: str) -> None:
        """Set ``active``, ``suspended``, or ``archived``; success has no body."""
        self._transport.request_json(
            "PATCH", f"/management/children/{enc(child_id)}/state", json={"state": state}
        )

    def create_environment(self, child_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        envelope = self._transport.request_json(
            "POST", f"/management/children/{enc(child_id)}/environments", json=dict(body)
        )
        return envelope["data"]

    def create_api_key(self, child_id: str, env_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """Issue a child environment key within this credential's delegation ceiling."""
        envelope = self._transport.request_json(
            "POST",
            f"/management/children/{enc(child_id)}/environments/{enc(env_id)}/api-keys",
            json=dict(body),
        )
        return envelope["data"]

    def usage(self) -> dict[str, Any]:
        """Return account-wide usage; requires explicit all_children and usage:read."""
        envelope = self._transport.request_json("GET", "/management/usage")
        return envelope["data"]

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> HotManagementClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncHotManagementClient:
    """Root management API client. Requires an ``hm_`` token, issued by a root admin.

    Runtime API keys, service keys, and sessions cannot call these endpoints.
    Use a separate HotClient with a child environment key for runtime access.
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
            token=token, base_url=base_url, timeout=timeout, client=client
        )

    async def list_children(self, *, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        """Return ``children`` and ``total`` for the credential's authorized targets."""
        envelope = await self._transport.request_json(
            "GET", "/management/children", params={"limit": limit, "offset": offset}
        )
        return envelope["data"]

    async def create_child(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Provision a child using a required, retry-stable ``idempotency_key``."""
        envelope = await self._transport.request_json(
            "POST", "/management/children", json=dict(body)
        )
        return envelope["data"]

    async def set_child_state(self, child_id: str, state: str) -> None:
        """Set ``active``, ``suspended``, or ``archived``; success has no body."""
        await self._transport.request_json(
            "PATCH", f"/management/children/{enc(child_id)}/state", json={"state": state}
        )

    async def create_environment(self, child_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        envelope = await self._transport.request_json(
            "POST", f"/management/children/{enc(child_id)}/environments", json=dict(body)
        )
        return envelope["data"]

    async def create_api_key(
        self, child_id: str, env_id: str, body: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Issue a child environment key within this credential's delegation ceiling."""
        envelope = await self._transport.request_json(
            "POST",
            f"/management/children/{enc(child_id)}/environments/{enc(env_id)}/api-keys",
            json=dict(body),
        )
        return envelope["data"]

    async def usage(self) -> dict[str, Any]:
        """Return account-wide usage; requires explicit all_children and usage:read."""
        envelope = await self._transport.request_json("GET", "/management/usage")
        return envelope["data"]

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncHotManagementClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()
