"""Entity list MCP tools — named lists of proteins / peptides / genes / metabolites.

An entity list groups a fixed selection of identifiers drawn from one
or more datasets so a visualisation module can refer to that selection
by id (``proteinListId`` / ``entityListId``) instead of re-specifying
the set every time.

Endpoints exposed by the v2 API:

  POST   /api/workspaces/:workspace_id/entity_lists      → create
  GET    /api/workspaces/:workspace_id/entity_lists      → list (paginated)
  GET    /api/workspaces/:workspace_id/entity_lists/:id  → show
  PUT    /api/workspaces/:workspace_id/entity_lists/:id  → update
  DELETE /api/workspaces/:workspace_id/entity_lists/:id  → delete
"""

import json
from typing import Any, Dict, List

from md_python.models import EntityList

from .. import mcp
from .._client import get_client
from .._destructive import _attach_destructive


def _entity_list_to_dict(ell: EntityList) -> Dict[str, Any]:
    return {
        "id": str(ell.id),
        "name": ell.name,
        "type": ell.type,
        "experiment_id": str(ell.experiment_id) if ell.experiment_id else None,
        "items_count": ell.items_count,
        "owner": ell.owner,
        "items": [
            {
                "id": str(item.id) if item.id else None,
                "entity_id": item.entity_id,
                "group_id": item.group_id,
                "dataset_id": item.dataset_id,
            }
            for item in ell.items
        ],
        "created_at": ell.created_at.isoformat() if ell.created_at else None,
        "updated_at": ell.updated_at.isoformat() if ell.updated_at else None,
    }


@mcp.tool()
def create_entity_list(
    workspace_id: str,
    name: str,
    entity_type: str,
    items: List[Dict[str, Any]],
) -> str:
    """Create a named entity list in a workspace.

    Args:
      workspace_id: Parent workspace UUID.
      name: Display name. Must be non-empty.
      entity_type: One of ``protein``, ``peptide``, ``gene``, or
        ``metabolite``. Must match the entity type of the source
        dataset(s); a peptide list cannot be referenced by a
        protein-only module.
      items: At least one membership row. Each item is a dict with:
        - ``entity_id`` (str, required): the human-readable id, e.g.
          a protein-group accession or peptide sequence.
        - ``group_id`` (int): the dataset-internal group id (paired
          with dataset_id; both or neither must be present).
        - ``dataset_id`` (str): the source dataset UUID.

    Resolving items typically requires query_entities first — the LLM
    should fetch candidate (entity_id, group_id) pairs from the source
    dataset rather than ask the user to type them by hand.

    Returns the created list as JSON. The ``id`` is also surfaced in
    list_entity_lists for later lookup.
    """
    try:
        ell = get_client().workspaces.entity_lists.create(
            workspace_id=workspace_id,
            name=name,
            entity_type=entity_type,
            items=items,
        )
    except (ValueError, TypeError, Exception) as e:
        return f"Error: {e}"
    return (
        f"Entity list created. ID: {ell.id}\n"
        f"{json.dumps(_entity_list_to_dict(ell), indent=2)}"
    )


@mcp.tool()
def list_entity_lists(workspace_id: str, page: int = 1) -> str:
    """List entity lists in a workspace, paginated (50 per page).

    Args:
      workspace_id: Parent workspace UUID.
      page: 1-based page number (default 1).

    Returns JSON with shape::

        {
          "data": [<entity list dict>, ...],
          "pagination": {
            "current_page": int,
            "per_page": int,
            "total_count": int,
            "total_pages": int
          }
        }

    Each entity-list dict matches the shape returned by get_entity_list.
    """
    try:
        body = get_client().workspaces.entity_lists.list(
            workspace_id=workspace_id, page=page
        )
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)
    return json.dumps(
        {
            "data": [_entity_list_to_dict(ell) for ell in body["data"]],
            "pagination": body.get("pagination", {}),
        },
        indent=2,
    )


@mcp.tool()
def get_entity_list(workspace_id: str, list_id: str) -> str:
    """Fetch a single entity list by id (scoped to its workspace).

    Returns the list as JSON, including its items. Returns an
    ``{"error": ...}`` envelope when the id is unknown.
    """
    ell = get_client().workspaces.entity_lists.get(workspace_id, list_id)
    if ell is None:
        return json.dumps({"error": f"Entity list {list_id!r} not found"}, indent=2)
    return json.dumps(_entity_list_to_dict(ell), indent=2)


@mcp.tool()
def update_entity_list(
    workspace_id: str,
    list_id: str,
    name: str,
    entity_type: str,
    items: List[Dict[str, Any]],
) -> str:
    """Replace an entity list's name, entity_type, and items.

    The server performs a full replace — every field is required on
    every call. If you only want to change one field, call
    ``get_entity_list`` first to read the current state, then pass the
    unchanged fields back unchanged.

    Args:
      workspace_id: Parent workspace UUID.
      list_id: Entity list UUID to update.
      name: New display name (non-empty).
      entity_type: One of ``protein``, ``peptide``, ``gene``, or
        ``metabolite``.
      items: At least one membership row, same shape as
        create_entity_list.

    Returns the updated list as JSON. On validation / HTTP failure
    returns a prose error string starting with ``Error:``.
    """
    try:
        ell = get_client().workspaces.entity_lists.update(
            workspace_id=workspace_id,
            list_id=list_id,
            name=name,
            entity_type=entity_type,
            items=items,
        )
    except (ValueError, TypeError, Exception) as e:
        return f"Error: {e}"
    return json.dumps(_entity_list_to_dict(ell), indent=2)


@mcp.tool()
def delete_entity_list(workspace_id: str, list_id: str) -> str:
    """Permanently delete an entity list.

    DESTRUCTIVE: removes the list and breaks any saved module that
    references its ``proteinListId`` / ``entityListId``. Echo the list_id
    back to the user and wait for explicit ``yes, delete <id>``
    confirmation before calling — see the destructive-action mandate.

    Returns: prose ``"Entity list deleted successfully. ID: <id>"`` on
    success, or ``"Error: ..."`` on failure.
    """
    try:
        get_client().workspaces.entity_lists.delete(workspace_id, list_id)
    except Exception as e:
        return f"Error: {e}"
    return f"Entity list deleted successfully. ID: {list_id}"


_attach_destructive(delete_entity_list)
