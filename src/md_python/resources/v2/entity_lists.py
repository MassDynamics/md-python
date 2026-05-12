"""
EntityLists resource for the MD Python v2 client.

Maps the two endpoints exposed under a workspace:

  POST /api/workspaces/:workspace_id/entity_lists      → create
  GET  /api/workspaces/:workspace_id/entity_lists/:id  → show

There is no list/index endpoint yet (the server-side controller exposes
only Create and Show), so this resource intentionally does not implement
``list``. Looking up an entity list requires its id.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from ...models.entity_list import EntityList, EntityListItem

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
        entity_type: str,
        items: Sequence[EntityListItemInput],
    ) -> EntityList:
        """Create a named entity list inside a workspace.

        Args:
            workspace_id: Parent workspace UUID.
            name: Display name for the list.
            entity_type: One of ``protein``, ``peptide``, or ``gene``.
            items: At least one item. Each item is either an
                :class:`EntityListItem` or a dict with ``entity_id``,
                ``group_id`` and ``dataset_id``.

        Returns:
            The created :class:`EntityList` (with ``items`` populated).
        """
        if entity_type not in {"protein", "peptide", "gene"}:
            raise ValueError(
                "entity_type must be one of: protein, peptide, gene "
                f"(got {entity_type!r})"
            )
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
