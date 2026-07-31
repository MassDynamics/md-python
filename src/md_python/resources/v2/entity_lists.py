"""
EntityLists resource for the MD Python v2 client.

Maps the endpoints exposed under a workspace:

  POST /api/workspaces/:workspace_id/entity_lists      → create
  GET  /api/workspaces/:workspace_id/entity_lists      → list (paginated)
  GET  /api/workspaces/:workspace_id/entity_lists/:id  → show
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from ...models import Page
from ...models.entity_list import EntityList, EntityListItem, EntityType

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


_JSON_HEADERS = {"Content-Type": "application/json"}


def _check(response: Any, expected: int, action: str) -> None:
    if response.status_code != expected:
        raise Exception(f"Failed to {action}: {response.status_code} - {response.text}")


# A caller can pass items as a list of EntityListItem objects or as a
# list of plain dicts ({entity_id, group_id, dataset_id}). Both shapes
# are normalised before sending.
EntityListItemInput = Union[EntityListItem, Dict[str, Any]]


class EntityLists:
    """Workspace-scoped entity lists (proteins / peptides / genes).

    Reached via ``client.workspaces.entity_lists`` and always
    parameterised by ``workspace_id`` since lists live under a workspace.
    """

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def _base(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/entity_lists"

    def create(
        self,
        workspace_id: str,
        name: str,
        entity_type: EntityType,
        items: Sequence[EntityListItemInput],
    ) -> EntityList:
        """Create a named entity list inside a workspace.

        Args:
            workspace_id: Parent workspace UUID.
            name: Display name for the list.
            entity_type: An :class:`EntityType` — one of ``protein``,
                ``peptide``, ``gene``, ``metabolite`` or ``ptm``.
            items: At least one item. Each item is either an
                :class:`EntityListItem` or a dict with ``entity_id``,
                ``group_id`` and ``dataset_id``.

        Returns:
            The created :class:`EntityList` (with ``items`` populated).
        """
        if not items:
            raise ValueError("items must include at least one entry")

        payload_items: List[Dict[str, Any]] = []
        for item in items:
            if isinstance(item, EntityListItem):
                payload_items.append(item.to_create_payload())
            elif isinstance(item, dict):
                if "entity_id" not in item:
                    raise ValueError("every item must have an 'entity_id' field")
                payload_items.append(dict(item))
            else:
                raise TypeError(
                    "items entries must be EntityListItem or dict, "
                    f"got {type(item).__name__}"
                )

        response = self._client._make_request(
            method="POST",
            endpoint=self._base(workspace_id),
            json={
                "name": name,
                "entity_type": entity_type,
                "items": payload_items,
            },
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create entity list")
        return EntityList.from_json(response.json())

    def list(self, workspace_id: str, page: int = 1) -> Page[EntityList]:
        """List entity lists in a workspace, paginated.

        Returns the paginated envelope ``{"data": [...], "pagination": {...}}``
        with the ``data`` items decoded into :class:`EntityList` objects.
        """
        response = self._client._make_request(
            method="GET",
            endpoint=self._base(workspace_id),
            params={"page": page},
        )
        _check(response, 200, "list entity lists")
        body = response.json()
        return {
            "data": [EntityList.from_json(x) for x in body.get("data", [])],
            "pagination": body.get("pagination", {}),
        }

    def list_all(self, workspace_id: str) -> List[EntityList]:
        """Convenience: page through all entity lists in a workspace."""
        out: List[EntityList] = []
        page = 1
        while True:
            body = self.list(workspace_id=workspace_id, page=page)
            out.extend(body["data"])
            pagination = body["pagination"]
            if page >= int(pagination.get("total_pages", page)):
                break
            page += 1
        return out

    def get(self, workspace_id: str, list_id: str) -> Optional[EntityList]:
        """Get a single entity list, or ``None`` if not found."""
        response = self._client._make_request(
            method="GET",
            endpoint=f"{self._base(workspace_id)}/{list_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get entity list")
        return EntityList.from_json(response.json())
