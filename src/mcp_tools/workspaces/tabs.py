"""Tab CRUD MCP tools.

A tab is a page inside a workspace. Each tab holds a layout of modules
placed on a react-grid-layout grid. ``tab_index`` is auto-assigned to
``max + 1`` on create. ``locked`` tabs reject updates and deletes.
"""

import json
from typing import Any, Dict, Optional

from md_python.models import Tab

from .. import mcp
from .._client import get_client
from .._destructive import _attach_destructive


def _tab_to_dict(t: Tab) -> Dict[str, Any]:
    return {
        "id": str(t.id),
        "workspace_id": str(t.workspace_id),
        "name": t.name,
        "settings": t.settings,
        "tab_index": t.tab_index,
        "locked": t.locked,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "updated_at": t.updated_at.isoformat() if t.updated_at else None,
    }


@mcp.tool()
def create_tab(
    workspace_id: str,
    name: str,
    settings: Optional[Dict[str, Any]] = None,
) -> str:
    """Create a new tab inside a workspace.

    REUSE-FIRST MANDATE (binding).
    Before calling this tool, the LLM MUST:
      1. Call list_tabs(workspace_id) and inspect the result.
      2. If ANY tab already exists — typically a default tab named
         "new tab" auto-created by the app UI when the workspace was
         first opened (see create_workspace docstring) — the LLM SHOULD
         reuse it (add_module_to_tab / update_tab against its tab_id)
         and NOT call create_tab. Use update_tab to rename it if a
         different display name is wanted.
      3. Only call create_tab when the user explicitly asks for a NEW
         additional tab (e.g. "add a second tab for QC plots") AND
         existing tabs do not fit that purpose.

    The auto-tab is created client-side at
    app/javascript/workspaces/repositories/WorkspaceTabsRepository.js#L8-28
    — the UI POSTs a tab named "new tab" with tab_index 0 the first
    time list_tabs returns empty. So:
      * A workspace JUST created via create_workspace has ZERO tabs.
      * A workspace the user has opened in the app has 1 tab named
        "new tab" already in place.
    The LLM should mirror this expectation when deciding whether to
    create_tab vs reuse.

    The server auto-assigns ``tab_index = max(tab_index) + 1`` and
    initialises the layout to ``{"modules": []}``. The newly created tab
    is unlocked.

    Args:
      workspace_id: Parent workspace id.
      name: Tab display name.
      settings: Optional free-form JSON dict (commonly used to set
                ``{"reportMode": true}`` for a print-style tab).

    Returns prose: ``Tab created. ID: <uuid>\\n<tab JSON>``
    """
    tab = get_client().workspaces.tabs.create(
        workspace_id, name=name, settings=settings
    )
    return f"Tab created. ID: {tab.id}\n{json.dumps(_tab_to_dict(tab), indent=2)}"


@mcp.tool()
def list_tabs(workspace_id: str, page: int = 1) -> str:
    """List tabs in a workspace, ordered by ``tab_index`` ascending.

    Returns JSON: ``{"data": [Tab, ...], "pagination": {...}}`` (50/page).
    """
    body = get_client().workspaces.tabs.list(workspace_id, page=page)
    return json.dumps(
        {
            "data": [_tab_to_dict(t) for t in body["data"]],
            "pagination": body["pagination"],
        },
        indent=2,
    )


@mcp.tool()
def get_tab(workspace_id: str, tab_id: str) -> str:
    """Fetch a single tab by id (scoped to its workspace)."""
    tab = get_client().workspaces.tabs.get(workspace_id, tab_id)
    if tab is None:
        return json.dumps({"error": f"Tab {tab_id!r} not found"}, indent=2)
    return json.dumps(_tab_to_dict(tab), indent=2)


@mcp.tool()
def update_tab(
    workspace_id: str,
    tab_id: str,
    name: Optional[str] = None,
    layout: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> str:
    """Partial update of a tab — only the fields you pass are sent.

    PUT replaces ``settings`` wholesale (no per-key merge); same for
    ``layout``. Locked tabs reject updates.

    Args:
      name: New display name.
      layout: New layout dict (e.g. ``{"modules": [...]}``). Prefer
              add/remove/update_tab_module unless you really need to
              replace the whole layout.
      settings: New settings dict.

    Returns the updated tab JSON.
    """
    if name is None and layout is None and settings is None:
        return json.dumps(
            {"error": "update_tab requires at least one of name/layout/settings"},
            indent=2,
        )
    tab = get_client().workspaces.tabs.update(
        workspace_id, tab_id, name=name, layout=layout, settings=settings
    )
    return json.dumps(_tab_to_dict(tab), indent=2)


@mcp.tool()
def delete_tab(workspace_id: str, tab_id: str) -> str:
    """Permanently delete a tab and every module inside its layout.

    Locked tabs reject deletion (the policy raises Pundit forbidden,
    surfaces as 403 here).

    Returns prose: ``Tab deleted successfully. ID: <uuid>``
    """
    get_client().workspaces.tabs.delete(workspace_id, tab_id)
    return f"Tab deleted successfully. ID: {tab_id}"


_attach_destructive(delete_tab)
