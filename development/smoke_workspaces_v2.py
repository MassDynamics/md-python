"""
Live smoke test for the v2 workspaces client.

Reads MD_API_BASE_URL / MD_AUTH_TOKEN from the .env in the working directory
(via python-dotenv inside BaseMDClient). Hits the live API and creates a
throwaway workspace + tab + module, then deletes everything.

Run from the worktree root::

    python development/smoke_workspaces_v2.py
"""

from __future__ import annotations

import sys
import time
import traceback

from md_python import MDClientV2


def header(msg: str) -> None:
    print(f"\n=== {msg} ===")


def main() -> int:
    client = MDClientV2()
    print(f"base_url={client.base_url}")

    workspace_id: str | None = None
    tab_id: str | None = None
    module_id: str | None = None

    try:
        header("health")
        print(client.health.check())

        header("module_registry.list")
        modules = client.module_registry.list()
        print(f"got {len(modules)} modules")
        if not modules:
            print("ERROR: module registry is empty — feature flag off?")
            return 2
        sample = next(
            (m for m in modules if m.id == "heading"),
            modules[0],
        )
        print(
            f"sample: id={sample.id} group={sample.group} keys={sample.setting_keys()}"
        )

        header("module_registry.get heading")
        heading = client.module_registry.get("heading")
        if heading is None:
            print("ERROR: 'heading' module not in registry for this user")
            return 2
        print(f"heading.input_settings keys: {heading.setting_keys()}")

        suffix = int(time.time())
        ws_name = f"smoke-test-workspace-{suffix}"

        header(f"workspaces.create name={ws_name!r}")
        ws = client.workspaces.create(
            name=ws_name,
            description="Created by md-python smoke test; safe to delete.",
        )
        workspace_id = str(ws.id)
        print(f"created workspace {workspace_id}")

        header("workspaces.get")
        fetched = client.workspaces.get(workspace_id)
        assert fetched is not None and str(fetched.id) == workspace_id
        print(f"round-tripped: name={fetched.name!r}")

        header("workspaces.list_all")
        all_ws = client.workspaces.list_all()
        print(f"user has {len(all_ws)} accessible workspaces")
        assert any(str(w.id) == workspace_id for w in all_ws), "new ws not in list"

        header("workspaces.tabs.create")
        tab = client.workspaces.tabs.create(workspace_id, name="Smoke Tab")
        tab_id = str(tab.id)
        print(f"created tab {tab_id}, tab_index={tab.tab_index}, locked={tab.locked}")

        header("workspaces.tabs.list")
        tabs_page = client.workspaces.tabs.list(workspace_id)
        print(f"  → {len(tabs_page['data'])} tab(s)")

        header("workspaces.modules.create_with_defaults heading")
        # Use create_with_defaults so the persisted module carries every key
        # the registry declares a default for. Sending just {text: "..."} via
        # plain create() persists a partial settings hash and the rendered
        # widget is broken (the API doesn't merge registry defaults
        # server-side — see dev-engineers note in the smoke-test docstring).
        mod = client.workspaces.modules.create_with_defaults(
            workspace_id=workspace_id,
            tab_id=tab_id,
            item_id="heading",
            x=0,
            y=0,
            width=12,
            height=1,
            settings={"text": "Hello from md-python smoke test"},
            registered_module=heading,  # save the extra GET; we already have it
        )
        module_id = str(mod.id)
        print(
            f"created module {module_id} item_id={mod.item_id} "
            f"({mod.width}x{mod.height} @ {mod.x},{mod.y})"
        )
        print(f"  persisted settings: {mod.settings}")
        # Belt-and-braces: assert every required key is present.
        for key in heading.required_setting_keys():
            assert key in mod.settings, f"required key {key!r} not persisted"

        header("workspaces.modules.list")
        listed = client.workspaces.modules.list(workspace_id, tab_id)
        print(f"  → {len(listed)} module(s)")
        assert any(str(m.id) == module_id for m in listed), "new module not in list"

        header("workspaces.modules.update — move to (1,0)")
        # Workaround for a server-side bug: the PUT endpoint reads
        # `existing['item_id']` but persistence stores `itemId` — so a partial
        # update without item_id resolves to nil and fails the presence check
        # ("item_id can't be blank"). Always re-send item_id on PUT until that
        # is fixed in the workflow repo.
        moved = client.workspaces.modules.update(
            workspace_id,
            tab_id,
            module_id,
            item_id=mod.item_id,
            x=1,
            y=0,
        )
        print(f"  → x={moved.x}, y={moved.y} (was 0,0)")
        assert moved.x == 1 and moved.y == 0

        header("workspaces.modules.get")
        got = client.workspaces.modules.get(workspace_id, tab_id, module_id)
        assert got is not None and got.x == 1
        print("  ok")

        header("PASS — all live calls round-tripped successfully")
        return 0

    except Exception:
        print("\n!!! SMOKE TEST FAILED !!!")
        traceback.print_exc()
        return 1

    finally:
        # Cleanup in reverse, best-effort. Each step swallows its own errors so
        # one stale leftover can't block the rest of the teardown.
        header("cleanup")
        if workspace_id and tab_id and module_id:
            try:
                client.workspaces.modules.delete(workspace_id, tab_id, module_id)
                print(f"deleted module {module_id}")
            except Exception as e:
                print(f"  module delete failed: {e}")
        if workspace_id and tab_id:
            try:
                client.workspaces.tabs.delete(workspace_id, tab_id)
                print(f"deleted tab {tab_id}")
            except Exception as e:
                print(f"  tab delete failed: {e}")
        if workspace_id:
            try:
                client.workspaces.delete(workspace_id)
                print(f"deleted workspace {workspace_id}")
            except Exception as e:
                print(f"  workspace delete failed: {e}")


if __name__ == "__main__":
    sys.exit(main())
