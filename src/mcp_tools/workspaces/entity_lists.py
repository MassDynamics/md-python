"""Entity list MCP tools — named lists of proteins / peptides / genes.

An entity list groups a fixed selection of identifiers drawn from one
or more datasets so a visualisation module can refer to that selection
by id (``proteinListId`` / ``entityListId``) instead of re-specifying
the set every time.

Two endpoints are exposed by the v2 API:

  POST /api/workspaces/:workspace_id/entity_lists      → create
  GET  /api/workspaces/:workspace_id/entity_lists/:id  → show

There is no list/index endpoint yet, so callers must remember the id
returned by create_entity_list (it is included in the tool's response).
"""

import json
from typing import Any, Dict, List

from md_python.models import EntityList

from .. import mcp
from .._client import get_client


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
    """Create a named entity list (proteins / peptides / genes) in a workspace.

    Args:
      workspace_id: Parent workspace UUID.
      name: Display name. Must be non-empty.
      entity_type: One of ``protein``, ``peptide``, or ``gene``. Must
        match the entity type of the source dataset(s); a peptide list
        cannot be referenced by a protein-only module.
      items: At least one membership row. Each item is a dict with:
        - ``entity_id`` (str, required): the human-readable id, e.g.
          a protein-group accession or peptide sequence.
        - ``group_id`` (int): the dataset-internal group id (paired
          with dataset_id; both or neither must be present).
        - ``dataset_id`` (str): the source dataset UUID.

    Resolving items typically requires query_entities first — the LLM
    should fetch candidate (entity_id, group_id) pairs from the source
    dataset rather than ask the user to type them by hand.

    Returns the created list as JSON. Save the ``id`` — there is no
    list/index endpoint, so it cannot be re-discovered after the call.
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
def get_entity_list(workspace_id: str, list_id: str) -> str:
    """Fetch a single entity list by id (scoped to its workspace).

    Returns the list as JSON, including its items. Returns an
    ``{"error": ...}`` envelope when the id is unknown.
    """
    ell = get_client().workspaces.entity_lists.get(workspace_id, list_id)
    if ell is None:
        return json.dumps({"error": f"Entity list {list_id!r} not found"}, indent=2)
    return json.dumps(_entity_list_to_dict(ell), indent=2)
