"""
One-shot rescue: rewrite ``{"values": [...]}`` settings into a flat list on
every existing module whose corresponding field is typed
``DatasetSampleMetadataValues``.

Background — pre-257c02d the MCP's _FIELD_TYPE_FALLBACKS wrapped this field
type as ``{"values": []}``, but the wire format is a flat
``List[Union[int, str]]``. The Dimensionality Reduction render bounced with
``Invalid sample_names type: <class 'dict'>. Expected List[Union[int, str]].``
(visualisations-service/src/requests/dimensionality_reduction_request.py:237).
Heatmap / DotPlot / Violin / ROC / EntityAbundance modules took the same
wrong fallback and were silently broken.

The MCP fix only changes what gets WRITTEN to newly placed modules. Modules
already on tabs still carry the bad shape in their persisted settings, and
must be patched in place. That's what this script does.

USAGE
-----
Dry-run (default — prints what would change, mutates nothing):

    python development/rescue_dataset_sample_metadata_values.py

Apply (issues PUT /workspaces/:ws/tabs/:tab/modules/:id with the corrected
settings hash):

    python development/rescue_dataset_sample_metadata_values.py --apply

Optional --workspace-id filters to one workspace; --module-id filters to a
single module (useful for spot-fixing one DimRed without touching the rest).

Reads MD_API_BASE_URL / MD_AUTH_TOKEN from the .env in the repo root.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional

from md_python import MDClientV2
from md_python.models import RegisteredModule


_BAD_FIELD_TYPE = "DatasetSampleMetadataValues"


def _input_settings_items(module: RegisteredModule):
    """Yield (key, spec_dict) for every input_settings entry, regardless of
    whether the registry serves the schema as a dict or as a list."""
    schema = module.input_settings
    if not schema:
        return
    if isinstance(schema, dict):
        for key, spec in schema.items():
            if isinstance(spec, dict):
                yield str(key), spec
    else:
        for spec in schema:
            if not isinstance(spec, dict):
                continue
            key = spec.get("key")
            if key is None:
                continue
            yield str(key), spec


def _dataset_sample_metadata_values_keys(module: RegisteredModule) -> List[str]:
    """Setting keys on this module backed by DatasetSampleMetadataValues."""
    out: List[str] = []
    for key, spec in _input_settings_items(module):
        field_type = spec.get("fieldType") or spec.get("type")
        if field_type == _BAD_FIELD_TYPE:
            out.append(key)
    return out


def _looks_like_bug_shape(value: Any) -> bool:
    """True when the persisted value is the exact bug shape: a dict whose
    only key is ``values`` mapping to a list. Be strict — never rewrite a
    legitimately-shaped envelope.

    Empty-dict ``{}`` is also rescued (vis-service rejects that too)."""
    if value == {}:
        return True
    return (
        isinstance(value, dict)
        and set(value.keys()) == {"values"}
        and isinstance(value["values"], list)
    )


def _rescue_value(value: Any) -> List[Any]:
    """Convert the bug-shape dict into the canonical flat list."""
    if value == {}:
        return []
    return list(value["values"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Issue PUT requests. Default is dry-run.",
    )
    parser.add_argument(
        "--workspace-id",
        default=None,
        help="Only inspect modules in this workspace.",
    )
    parser.add_argument(
        "--module-id",
        default=None,
        help="Only inspect this module id (still requires walking tabs).",
    )
    args = parser.parse_args()

    client = MDClientV2()
    print(f"base_url={client.base_url}  mode={'APPLY' if args.apply else 'DRY-RUN'}")
    print()

    # Pull the registry once so we can look up input_settings per item_id.
    registry_by_id: Dict[str, RegisteredModule] = {
        m.id: m for m in client.module_registry.list() if m.id
    }
    print(f"registry: {len(registry_by_id)} modules")

    # Walk workspaces → tabs → modules.
    if args.workspace_id:
        ws = client.workspaces.get(args.workspace_id)
        if ws is None:
            print(f"ERROR: workspace {args.workspace_id} not found")
            return 1
        workspaces = [ws]
    else:
        workspaces = client.workspaces.list_all()
    print(f"scanning {len(workspaces)} workspace(s)")
    print()

    inspected = 0
    rescued = 0
    rescued_modules: List[Dict[str, Any]] = []

    for ws in workspaces:
        tabs = client.workspaces.tabs.list_all(workspace_id=ws.id)
        for tab in tabs:
            modules = client.workspaces.modules.list(
                workspace_id=ws.id, tab_id=tab.id
            )
            for mod in modules:
                if args.module_id and mod.id != args.module_id:
                    continue
                inspected += 1
                registered = registry_by_id.get(mod.item_id)
                if registered is None:
                    continue
                target_keys = _dataset_sample_metadata_values_keys(registered)
                if not target_keys:
                    continue

                # Find keys whose stored value is the bug shape.
                bad_keys = [
                    k for k in target_keys
                    if k in mod.settings and _looks_like_bug_shape(mod.settings[k])
                ]
                if not bad_keys:
                    continue

                # Rebuild the full settings hash with the corrected values.
                new_settings = dict(mod.settings)
                changes: List[str] = []
                for k in bad_keys:
                    before = mod.settings[k]
                    after = _rescue_value(before)
                    new_settings[k] = after
                    changes.append(f"      {k}: {before!r} -> {after!r}")

                rescued_modules.append(
                    {
                        "workspace_id": ws.id,
                        "workspace_name": ws.name,
                        "tab_id": tab.id,
                        "tab_name": tab.name,
                        "module_id": mod.id,
                        "module_item_id": mod.item_id,
                        "changes": changes,
                    }
                )

                print(
                    f"  workspace={ws.name!r} ({ws.id})\n"
                    f"    tab={tab.name!r} ({tab.id})\n"
                    f"      module={mod.item_id} ({mod.id})"
                )
                for line in changes:
                    print(line)

                if args.apply:
                    client.workspaces.modules.update(
                        workspace_id=ws.id,
                        tab_id=tab.id,
                        module_id=mod.id,
                        item_id=mod.item_id,
                        settings=new_settings,
                    )
                    print("      [APPLIED]")
                    rescued += 1

    print()
    print(f"inspected: {inspected} module(s)")
    print(f"would rescue: {len(rescued_modules)} module(s)")
    if args.apply:
        print(f"applied:    {rescued} module(s)")
    else:
        print("dry-run — pass --apply to actually update.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
