"""
Module registry resource for the MD Python v2 client.

Mirrors `/api/module_registry/modules` (see
`app/api/api/v2/module_registry/`). Use this to discover the dashboard
module catalogue and inspect each module's ``input_settings`` schema before
placing modules on a tab.
"""

from typing import TYPE_CHECKING, List, Optional

from ...models import RegisteredModule

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class ModuleRegistry:
    """V2 module registry resource."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def list(self) -> List[RegisteredModule]:
        """List dashboard modules available to the current user.

        The server applies feature-flag filtering before returning, so the
        result is the set of modules the caller can actually place on a tab.
        """
        response = self._client._make_request(
            method="GET",
            endpoint="/module_registry/modules",
        )

        if response.status_code == 200:
            payload = response.json()
            data = payload.get("data", []) if isinstance(payload, dict) else payload
            return [RegisteredModule.from_json(m) for m in data]

        raise Exception(
            f"Failed to list registry modules: "
            f"{response.status_code} - {response.text}"
        )

    def get(self, item_id: str) -> Optional[RegisteredModule]:
        """Get a registered module by id, or ``None`` if not found/available."""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/module_registry/modules/{item_id}",
        )

        if response.status_code == 404:
            return None
        if response.status_code == 200:
            return RegisteredModule.from_json(response.json())

        raise Exception(
            f"Failed to get registry module {item_id}: "
            f"{response.status_code} - {response.text}"
        )
