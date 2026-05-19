"""Top-level ``describe()`` — the assembled module-type doc for the LLM."""

from __future__ import annotations

from typing import Any, Dict

from md_python.models import RegisteredModule

from .dataset_inputs import dataset_input_for, entity_type_input_for
from .parameter_docs import _aggregate_data_dependencies, parameters_for


def describe(module: RegisteredModule) -> Dict[str, Any]:
    """Full structured description of a module type for the LLM.

    The returned shape (verbatim — every key always present, never elided):

    {
      "id":  "...",                       # module item_id
      "name": "...",
      "short_name": "...",
      "group": "...",                     # registry category
      "icon":  "...",
      "keywords": [...],
      "instruction_name": "...",
      "description": "<long-form prose — use this verbatim when explaining
                       the module to the user>",
      "short_description": "...",
      "parameters": [
        {
          "key": "...",
          "name": "...",
          "group": "...",
          "field_type": "...",
          "value_kind": "...",
          "value_description": "...",
          "platform_default": ...,
          "default_present": bool,
          "default_note": "..." | null,
          "is_required": bool,
          "data_dependencies": [...],
          "cross_field_refs": [...],
          "options": [{value, label}] | null,
          "condition": {...} | null,
          "description": "..." | null,
          "fillable_by_llm": bool,
          "raw_parameters": {...} | null,
        },
        ...
      ],
      "data_dependencies": [...],         # union across parameters
      "required_keys_no_default": [...],  # required AND no default —
                                          # the LLM MUST collect a value
      "registry_defaults": {key: value, ...},  # what create_with_defaults
                                               # would auto-send
    }
    """
    params = parameters_for(module)
    deps = _aggregate_data_dependencies(params)
    return {
        "id": module.id,
        "name": module.name,
        "short_name": module.short_name,
        "group": module.group,
        "icon": module.icon,
        "keywords": list(module.keywords),
        "instruction_name": module.instruction_name,
        "description": module.description,
        "short_description": module.short_description,
        "parameters": params,
        "data_dependencies": deps,
        "required_keys_no_default": module.missing_required_keys({}),
        "registry_defaults": module.defaults(),
        # Top-level summary of the module's dataset binding — pulled out
        # from input_settings.<datasetsSearch>.parameters for the LLM
        # and for the add_module_to_tab tool to validate against.
        "dataset_input": dataset_input_for(module),
        # Top-level summary of the module's entity_type binding (if any).
        # Most plot modules need entity_type ∈ {protein, peptide, gene}
        # and there is no default — the LLM MUST supply a value.
        "entity_type_input": entity_type_input_for(module),
    }
