"""Live smoke test for the visualisation MCP tools.

Drives the entire visualise flow through the MCP tool surface (not the raw
Python client) — same shape Claude Desktop calls would take. Creates a
throwaway workspace + tab, places a heading and a PCA plot, exercises
list/get/update/remove, then cleans up.

Reads MD_AUTH_TOKEN / MD_API_BASE_URL from .env. Run from the worktree
root::

    python development/smoke_workspaces_mcp.py
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from pathlib import Path

# Resolve .env relative to the script (same trick as mcp_server.py).
from mcp_tools._env import load_env_from

load_env_from(Path(__file__).resolve().parent.parent)

from mcp_tools.workspaces.crud import (  # noqa: E402
    create_workspace,
    delete_workspace,
)
from mcp_tools.workspaces.modules import (  # noqa: E402
    add_module_to_tab,
    list_tab_modules,
    remove_module_from_tab,
    update_tab_module,
)
from mcp_tools.workspaces.registry import (  # noqa: E402
    describe_module_type,
    list_module_types,
)
from mcp_tools.workspaces.tabs import create_tab, delete_tab  # noqa: E402


def header(msg: str) -> None:
    print(f"\n=== {msg} ===")


def call(tool: object, *args: object, **kwargs: object) -> str:
    """Call an MCP tool and parse its return — JSON or prose-with-ID."""
    fn = getattr(tool, "fn", None) or tool
    out: str = fn(*args, **kwargs)  # type: ignore[operator]
    print(out[:600] + ("\n  ..." if len(out) > 600 else ""))
    return out


def extract_id(prose: str) -> str:
    """Pull the UUID after `ID: ` in the standard prose return shape."""
    for line in prose.splitlines():
        if "ID:" in line:
            return line.split("ID:", 1)[1].strip()
    raise ValueError(f"no ID found in prose:\n{prose}")


def main() -> int:
    workspace_id = tab_id = module_id = pca_module_id = None
    try:
        header("list_module_types — verify the visualise discovery layer")
        out = call(list_module_types)
        types = json.loads(out)
        print(
            f"  → {types['total']} modules, "
            f"groups={list(types['groups'].keys())[:5]}..."
        )
        assert types["total"] > 0

        header("describe_module_type heading — verify rich docs")
        out = call(describe_module_type, "heading")
        described = json.loads(out)
        # Every parameter doc has the full schema — schema-coverage check
        for p in described["parameters"]:
            assert "data_dependencies" in p
            assert "fillable_by_llm" in p
            assert "default_note" in p
            assert "is_required" in p
        print(
            f"  → {len(described['parameters'])} parameters, "
            f"required-no-default={described['required_keys_no_default']}, "
            f"defaults={list(described['registry_defaults'].keys())}"
        )

        header("describe_module_type dimensionality_reduction_plot — PCA case")
        # The user's exemplar: PCA's colourBy needs sample_metadata. The
        # data_dependencies block must surface that explicitly.
        out = call(describe_module_type, "dimensionality_reduction_plot")
        pca = json.loads(out)
        deps_joined = " ".join(pca["data_dependencies"]).lower()
        assert "sample_metadata" in deps_joined, (
            "PCA must declare sample_metadata as a data dependency "
            "(driven by the colourBy field)"
        )
        assert "dataset" in deps_joined
        print(f"  → data_dependencies={pca['data_dependencies']}")

        suffix = int(time.time())
        header(f"create_workspace — smoke-mcp-{suffix}")
        ws_prose = call(
            create_workspace,
            name=f"smoke-mcp-{suffix}",
            description="Created by smoke_workspaces_mcp.py — safe to delete.",
        )
        workspace_id = extract_id(ws_prose)
        print(f"  → workspace_id={workspace_id}")

        header("create_tab")
        tab_prose = call(create_tab, workspace_id, name="Visualise Smoke")
        tab_id = extract_id(tab_prose)
        print(f"  → tab_id={tab_id}")

        header("add_module_to_tab heading — defaults baked in")
        mod_prose = call(
            add_module_to_tab,
            workspace_id,
            tab_id,
            "heading",
            x=0,
            y=0,
            width=12,
            height=1,
            settings={"text": "Smoke test heading via MCP"},
        )
        module_id = extract_id(mod_prose)
        # Verify the persisted settings include EVERY registry-declared key.
        mod_obj = json.loads(mod_prose.split("\n", 1)[1])
        assert (
            mod_obj["settings"]["size"] == "h1"
        ), f"size default not baked in: {mod_obj['settings']}"
        assert mod_obj["settings"]["horizontalPosition"] == "left"
        assert mod_obj["settings"]["verticalPosition"] == "middle"
        assert mod_obj["settings"]["text"] == "Smoke test heading via MCP"
        print(f"  → persisted full settings hash, module_id={module_id}")

        header("add_module_to_tab — fail-fast on missing required-no-default")
        # Heading.text is required and has no registry default. Calling
        # add_module_to_tab without text MUST fail-fast (no API roundtrip).
        out = call(
            add_module_to_tab,
            workspace_id,
            tab_id,
            "heading",
            x=0,
            y=2,
            width=12,
            height=1,
        )
        assert out.startswith("Error: "), (
            "add_module_to_tab must fail-fast when a required-no-default "
            f"key is missing; got: {out}"
        )
        assert "text" in out

        header("list_tab_modules")
        out = call(list_tab_modules, workspace_id, tab_id)
        listed = json.loads(out)
        assert any(m["id"] == module_id for m in listed["data"])

        header("update_tab_module — move + rebuild full settings")
        # Replicate the exact workaround the docstring tells the LLM about:
        # PUT replaces settings wholesale, so we rebuild from defaults +
        # user changes.
        described_h = json.loads(call(describe_module_type, "heading"))
        new_settings = {
            **described_h["registry_defaults"],
            "text": "Moved + resized via MCP",
            "size": "h2",
        }
        out = call(
            update_tab_module,
            workspace_id,
            tab_id,
            module_id,
            item_id="heading",  # mandatory on PUT (server-side bug workaround)
            x=2,
            y=3,
            width=8,
            height=2,
            settings=new_settings,
        )
        moved = json.loads(out)
        assert moved["x"] == 2 and moved["y"] == 3
        assert moved["settings"]["size"] == "h2"
        assert moved["settings"]["text"] == "Moved + resized via MCP"
        # Defaults still present
        assert moved["settings"]["horizontalPosition"] == "left"

        header("PASS — visualise MCP flow round-tripped successfully")
        return 0

    except Exception:
        print("\n!!! VISUALISE MCP SMOKE TEST FAILED !!!")
        traceback.print_exc()
        return 1

    finally:
        header("cleanup")
        # Best-effort, in reverse order.
        if workspace_id and tab_id and pca_module_id:
            try:
                call(remove_module_from_tab, workspace_id, tab_id, pca_module_id)
            except Exception as e:
                print(f"  pca module remove failed: {e}")
        if workspace_id and tab_id and module_id:
            try:
                call(remove_module_from_tab, workspace_id, tab_id, module_id)
            except Exception as e:
                print(f"  module remove failed: {e}")
        if workspace_id and tab_id:
            try:
                call(delete_tab, workspace_id, tab_id)
            except Exception as e:
                print(f"  tab delete failed: {e}")
        if workspace_id:
            try:
                call(delete_workspace, workspace_id)
            except Exception as e:
                print(f"  workspace delete failed: {e}")


if __name__ == "__main__":
    sys.exit(main())
