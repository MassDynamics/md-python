"""Tabs resource — children of a workspace."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ....models import Tab
from ._common import _JSON_HEADERS, _PAGE_RESPONSE, _check

if TYPE_CHECKING:
    from ....base_client import BaseMDClient


class Tabs:
    """Tabs inside a workspace. Reached via ``client.workspaces.tabs``."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def _base(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/tabs"

    def create(
        self,
        workspace_id: str,
        name: str,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Tab:
        """Create a tab. ``tab_index`` is auto-assigned to ``max + 1``."""
        payload: Dict[str, Any] = {"name": name}
        if settings is not None:
            payload["settings"] = settings

        response = self._client._make_request(
            method="POST",
            endpoint=self._base(workspace_id),
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create tab")
        return Tab.from_json(response.json())

    def list(self, workspace_id: str, page: int = 1) -> _PAGE_RESPONSE:
        """List tabs in a workspace, paginated and ordered by ``tab_index`` asc.

        Returns the raw paginated envelope::

            {"data": [Tab, ...], "pagination": {...}}

        with the ``data`` items decoded into :class:`Tab` objects.
        """
        response = self._client._make_request(
            method="GET",
            endpoint=self._base(workspace_id),
            params={"page": page},
        )
        _check(response, 200, "list tabs")
        body = response.json()
        return {
            "data": [Tab.from_json(t) for t in body.get("data", [])],
            "pagination": body.get("pagination", {}),
        }

    def list_all(self, workspace_id: str) -> List[Tab]:
        """Convenience: page through all tabs in a workspace."""
        out: List[Tab] = []
        page = 1
        while True:
            body = self.list(workspace_id=workspace_id, page=page)
            out.extend(body["data"])
            pagination = body["pagination"]
            if page >= int(pagination.get("total_pages", page)):
                break
            page += 1
        return out

    def get(self, workspace_id: str, tab_id: str) -> Optional[Tab]:
        response = self._client._make_request(
            method="GET",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get tab")
        return Tab.from_json(response.json())

    def update(
        self,
        workspace_id: str,
        tab_id: str,
        name: Optional[str] = None,
        layout: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Tab:
        """Partial update. Locked tabs reject updates."""
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if layout is not None:
            payload["layout"] = layout
        if settings is not None:
            payload["settings"] = settings

        response = self._client._make_request(
            method="PUT",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 200, "update tab")
        return Tab.from_json(response.json())

    def delete(self, workspace_id: str, tab_id: str) -> bool:
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
        )
        _check(response, 204, "delete tab")
        return True
