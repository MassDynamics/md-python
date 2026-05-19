"""Workspaces resource — the top-level visual container."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ....models import Workspace
from ..entity_lists import EntityLists
from ._common import _JSON_HEADERS, _PAGE_RESPONSE, _check
from .tab_modules import TabModules
from .tabs import Tabs

if TYPE_CHECKING:
    from ....base_client import BaseMDClient
    from ..module_registry import ModuleRegistry


class Workspaces:
    """V2 workspaces resource — the entry point for the visual environment.

    Tabs and tab-modules are reached via the nested resources::

        client.workspaces.create(name="...")
        client.workspaces.tabs.create(workspace_id, name="...")
        client.workspaces.modules.create(workspace_id, tab_id, ...)
    """

    def __init__(
        self,
        client: "BaseMDClient",
        registry: Optional["ModuleRegistry"] = None,
    ):
        self._client = client
        self.tabs = Tabs(client)
        self.modules = TabModules(client, registry=registry)
        self.entity_lists = EntityLists(client)

    def create(self, name: str, description: Optional[str] = None) -> Workspace:
        payload: Dict[str, Any] = {"name": name}
        if description is not None:
            payload["description"] = description

        response = self._client._make_request(
            method="POST",
            endpoint="/workspaces",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create workspace")
        return Workspace.from_json(response.json())

    def list(self, page: int = 1) -> _PAGE_RESPONSE:
        """List workspaces accessible to the current user, 50 per page."""
        response = self._client._make_request(
            method="GET",
            endpoint="/workspaces",
            params={"page": page},
        )
        _check(response, 200, "list workspaces")
        body = response.json()
        return {
            "data": [Workspace.from_json(w) for w in body.get("data", [])],
            "pagination": body.get("pagination", {}),
        }

    def list_all(self) -> List[Workspace]:
        """Convenience: page through all accessible workspaces."""
        out: List[Workspace] = []
        page = 1
        while True:
            body = self.list(page=page)
            out.extend(body["data"])
            pagination = body["pagination"]
            if page >= int(pagination.get("total_pages", page)):
                break
            page += 1
        return out

    def get(self, workspace_id: str) -> Optional[Workspace]:
        response = self._client._make_request(
            method="GET",
            endpoint=f"/workspaces/{workspace_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get workspace")
        return Workspace.from_json(response.json())

    def update(
        self,
        workspace_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Workspace:
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description

        response = self._client._make_request(
            method="PUT",
            endpoint=f"/workspaces/{workspace_id}",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 200, "update workspace")
        return Workspace.from_json(response.json())

    def delete(self, workspace_id: str) -> bool:
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/workspaces/{workspace_id}",
        )
        _check(response, 204, "delete workspace")
        return True
