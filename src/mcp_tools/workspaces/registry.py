"""Module-registry MCP tools.

Discovery layer for the visualisation flow:

  * list_module_types     — index of every dashboard module the user can
                            place. Returns id, name, group, short
                            description, keywords, and which platform
                            defaults exist (so the LLM can decide whether to
                            call describe_module_type next).
  * describe_module_type  — full structured documentation for one module
                            type. Every parameter is documented — even
                            null defaults, even optional fields. This is
                            the source-of-truth that the parameter Q&A
                            mandate is grounded against.
"""

import json

from .. import mcp
from .._client import get_client
from . import _introspect
from ._renderable import is_renderable


@mcp.tool()
def list_module_types() -> str:
    """List every dashboard module type available to the current user.

    Use this to discover which modules can be placed on a tab. The list is
    filtered server-side by feature flags, so it reflects what the calling
    user can actually use.

    Returns JSON with the shape::

        {
          "data": [
            {"id": "...", "name": "...", "short_name": "...",
             "group": "...", "icon": "...", "keywords": [...],
             "short_description": "...",
             "has_registry_defaults": bool,
             "required_keys_no_default": [...],
             "dataset_input": {...} | null,
             "entity_type_input": {...} | null,
             "renderable": bool}
          ],
          "total": int,
          "groups": {group_name: count, ...}
        }

    ``required_keys_no_default`` is the list of parameter keys the LLM
    MUST collect from the user (e.g. ``datasetsSearch`` for any data-bound
    plot) — add_module_to_tab fails fast on any of them that is missing.
    ``has_registry_defaults`` tells the LLM whether create_with_defaults
    will fill anything in.

    ``entity_type_input`` is the module's entity_type contract, published
    here so it costs no extra round-trip::

        null                     -> the module accepts NO entity_type.
                                    Passing one to add_module_to_tab drops
                                    it (with a warning). Do not pass it.
        {"required": true,
         "valid_values": [...]}  -> add_module_to_tab NEEDS entity_type,
                                    and only these values (per-module —
                                    the accepted set is not global).

    ``renderable`` says whether render_module_visualisation can render the
    module type at all. False means the module is placeable and valid but
    UI-only — it draws when the user opens the workspace, and calling
    render_module_visualisation on it will fail. Only 12 module types are
    renderable.

    Next step: pick an item_id and call describe_module_type for full
    parameter docs before calling add_module_to_tab.
    """
    modules = get_client().module_registry.list()
    by_group: dict = {}
    out: list = []
    for m in modules:
        defaults = m.defaults()
        out.append(
            {
                "id": m.id,
                "name": m.name,
                "short_name": m.short_name,
                "group": m.group,
                "icon": m.icon,
                "keywords": list(m.keywords),
                "short_description": m.short_description,
                "has_registry_defaults": bool(defaults),
                "required_keys_no_default": m.missing_required_keys({}),
                # Dataset binding summary — single vs multiple, type required.
                # The LLM uses this to filter ("show me all PAIRWISE modules")
                # and to decide which add_module_to_tab args to pass.
                "dataset_input": _introspect.dataset_input_for(m),
                # entity_type contract. Published HERE (not only in
                # describe_module_type) because the LLM reads this index far
                # more often than it describes a single module — and
                # entity_type is otherwise undiscoverable until a failed
                # add_module_to_tab. null = the module takes none.
                "entity_type_input": _introspect.entity_type_input_for(m),
                # Whether render_module_visualisation works for this module
                # type at all (mirrors the vis-service REGISTRY).
                "renderable": is_renderable(m.id),
            }
        )
        by_group[m.group] = by_group.get(m.group, 0) + 1
    return json.dumps(
        {"data": out, "total": len(out), "groups": by_group},
        indent=2,
    )


@mcp.tool()
def describe_module_type(item_id: str) -> str:
    """Full parameter documentation for one dashboard module type.

    Returns the structured doc the parameter Q&A mandate is grounded
    against — every parameter listed (even null defaults, even optional
    fields), with field type, value semantics, data dependencies,
    conditional-visibility clauses, cross-field references, options, and
    the prose description from the registry.

    The returned JSON has the shape::

        {
          "id": "<item_id>",
          "name": "...",
          "short_name": "...",
          "group": "...",
          "icon": "...",
          "keywords": [...],
          "instruction_name": "...",
          "description": "<long-form prose — quote verbatim>",
          "short_description": "...",
          "parameters": [
            {
              "key": "...",
              "name": "...",
              "group": "...",
              "field_type": "...",
              "value_kind": "...",                 # short label
              "value_description": "...",          # plain-language sentence
              "platform_default": ...,             # MD canonical default
              "default_present": bool,
              "default_note": "..." | null,        # null/missing default flags
              "is_required": bool,
              "data_dependencies": [...],          # what to fetch first
              "cross_field_refs": [...],           # other params it depends on
              "options": [{value, label}] | null,  # enum choices
              "condition": {...} | null,           # `when` clause
              "description": "..." | null,         # parameter prose
              "fillable_by_llm": bool,
              "raw_parameters": {...} | null
            }, ...
          ],
          "data_dependencies": [...],         # union across all parameters
          "required_keys_no_default": [...],  # MUST be collected from user
          "registry_defaults": {key: value, ...},
          "dataset_input": {...} | null,      # dataset binding contract
          "entity_type_input": {...} | null,  # entity_type contract;
                                              # null = takes NO entity_type
          "renderable": bool,                 # render_module_visualisation
                                              # works on this module type?
          "render_note": "..." | null         # why not, when not renderable
        }

    ``entity_type_input.valid_values`` is the set THIS module accepts —
    it is not a global list. ``renderable: false`` means the module is
    valid and draws in the web UI but has no server-side renderer: place
    it, then tell the user to open the workspace; do NOT call
    render_module_visualisation on it.

    The ``data_dependencies`` block is the LLM's checklist: every entry is
    something the LLM must know about before it can sensibly fill values.
    Examples:
      * "sample_metadata for the upload (call get_upload_sample_metadata)"
        — when the module has a colourBy / xAxis / sampleNames parameter.
      * "the chosen dataset (referenced in parameters.datasetsSearch.ref)"
        — when an EntityType or ProteinList parameter depends on the
        dataset.
      * "a PAIRWISE dataset" / "an INTENSITY dataset" — encoded in the
        parameters.type of the Datasets field.

    Next step: present every parameter to the user (the visualisation
    mandate) and call add_module_to_tab with the confirmed values.

    Returns: JSON string. Error envelope ``{"error": "..."}`` when the id
    is unknown to the registry / not available to the current user.
    """
    mod = get_client().module_registry.get(item_id)
    if mod is None:
        return json.dumps(
            {
                "error": (
                    f"item_id {item_id!r} is not in the module registry "
                    "(or is not available to the current user). "
                    "Call list_module_types to see available ids."
                )
            },
            indent=2,
        )
    return json.dumps(_introspect.describe(mod), indent=2)
