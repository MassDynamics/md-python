"""Workspace CRUD MCP tools.

A workspace is the top-level container for tabs in the visual environment.
Use these tools to create, list, fetch, rename, and delete workspaces.
"""

import json
from typing import Any, Dict, Optional

from md_python.models import Workspace

from .. import mcp
from .._client import get_client
from .._destructive import _attach_destructive


def _workspace_to_dict(ws: Workspace) -> Dict[str, Any]:
    return {
        "id": str(ws.id),
        "name": ws.name,
        "description": ws.description,
        "created_at": ws.created_at.isoformat() if ws.created_at else None,
        "updated_at": ws.updated_at.isoformat() if ws.updated_at else None,
    }


@mcp.tool()
def create_workspace(name: str, description: Optional[str] = None) -> str:
    """Create a new workspace.

    A workspace is the top-level container for tabs in the visual
    environment. It is a PURELY VISUAL container — it does NOT own,
    contain, or store uploads, datasets, or pipeline runs. Uploads and
    datasets are owned by the user at the account level and live
    independently of any workspace. Modules placed on a workspace tab
    REFERENCE existing dataset_ids by id. Do NOT create a workspace as
    a prerequisite for uploading data or running a pipeline; only call
    this tool when the user explicitly wants to VIEW results.

    After creating a workspace, place modules with add_module_to_tab —
    but FIRST call list_tabs and REUSE the existing tab if one is there
    (see "auto-tab" note below).

    AUTO-TAB BEHAVIOUR (lazy, frontend-driven). The Mass Dynamics app
    auto-creates a single default tab named "new tab" with tab_index 0
    the first time the workspace is opened in the UI. The auto-creation
    is implemented client-side at app/javascript/workspaces/repositories/
    WorkspaceTabsRepository.js#L8-28 — when the UI calls list_tabs and
    gets an empty list, it POSTs the default tab and re-fetches.

    Implications for the LLM:
      * A workspace JUST created via this tool has ZERO tabs — the API
        does not auto-populate anything.
      * If the user has already opened the workspace in the app, exactly
        one tab named "new tab" already exists.
      * Therefore: ALWAYS call list_tabs(workspace_id) before create_tab.
        If a tab is already there, REUSE it (add_module_to_tab against
        its tab_id) — do not create a parallel default tab.

    Args:
      name: Display name (required).
      description: Optional free-form description.

    Returns prose: ``Workspace created. ID: <uuid>\\n<workspace JSON>``
    """
    ws = get_client().workspaces.create(name=name, description=description)
    return (
        f"Workspace created. ID: {ws.id}\n"
        f"{json.dumps(_workspace_to_dict(ws), indent=2)}"
    )


@mcp.tool()
def list_workspaces(page: int = 1) -> str:
    """List workspaces accessible to the current user.

    Args:
      page: 1-indexed page number, 50 results per page.

    Returns JSON: ``{"data": [Workspace, ...], "pagination": {...}}``
    """
    body = get_client().workspaces.list(page=page)
    return json.dumps(
        {
            "data": [_workspace_to_dict(w) for w in body["data"]],
            "pagination": body["pagination"],
        },
        indent=2,
    )


@mcp.tool()
def get_workspace(workspace_id: str) -> str:
    """Fetch a single workspace by id.

    Returns JSON of the workspace, or ``{"error": "..."}`` on 404.
    """
    ws = get_client().workspaces.get(workspace_id)
    if ws is None:
        return json.dumps({"error": f"Workspace {workspace_id!r} not found"}, indent=2)
    return json.dumps(_workspace_to_dict(ws), indent=2)


@mcp.tool()
def update_workspace(
    workspace_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Update a workspace's name and/or description (partial — only
    the fields you pass are sent).

    Returns the updated workspace JSON.
    """
    if name is None and description is None:
        return json.dumps(
            {"error": "update_workspace requires at least one of name/description"},
            indent=2,
        )
    ws = get_client().workspaces.update(
        workspace_id, name=name, description=description
    )
    return json.dumps(_workspace_to_dict(ws), indent=2)


@mcp.tool()
def delete_workspace(workspace_id: str) -> str:
    """Permanently delete a workspace and every tab + module inside it.

    Cascades on the server side — every Tab is delete_all'd and every
    module persisted in tab.layout disappears with it.

    Returns prose: ``Workspace deleted successfully. ID: <uuid>``
    """
    get_client().workspaces.delete(workspace_id)
    return f"Workspace deleted successfully. ID: {workspace_id}"


_attach_destructive(delete_workspace)
