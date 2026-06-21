"""Dataset / entity-type input extraction and fallback merging.

Helpers that surface top-level summaries the ``add_module_to_tab`` tool
uses to validate the LLM's structured arguments (``dataset_id``,
``upload_id``, ``entity_type``) against the module's registry spec, plus
the field-type-level fallback overlay for required-no-default fields.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from md_python.models import RegisteredModule

from .parameter_docs import _is_required


def entity_type_input_for(module: RegisteredModule) -> Optional[Dict[str, Any]]:
    """Find the EntityType-typed parameter on a module, if any.

    Most plot modules carry an ``entityType`` field that says whether the
    dataset is being interpreted as protein, peptide, gene, or metabolite
    level. This is required-no-default for almost every plot — the LLM has
    to supply it. Returns the structured block for the tool layer or None
    when the module has no EntityType field.

    The registry does NOT enumerate which entity types a given module
    accepts (the EntityType field carries ``options: null`` server-side —
    the value-space is resolved from the chosen dataset at render time).
    So ``valid_values`` is the full client-side vocabulary; vis-service is
    the final arbiter and will reject an unsupported entity_type at
    render_module_visualisation time.
    """
    schema = module.input_settings
    if not schema:
        return None
    if isinstance(schema, dict):
        items = list(schema.items())
    else:
        items = [(str(s.get("key")), s) for s in schema if isinstance(s, dict)]
    for key, spec in items:
        if not isinstance(spec, dict):
            continue
        field_type = spec.get("fieldType") or spec.get("type")
        if field_type != "EntityType":
            continue
        return {
            "settings_key": key,
            "required": _is_required(spec),
            "valid_values": ["protein", "peptide", "gene", "metabolite"],
            "tool_arg": "entity_type",
        }
    return None


def dataset_input_for(module: RegisteredModule) -> Optional[Dict[str, Any]]:
    """Find the Datasets-typed parameter on a module, if any.

    Most plot modules accept exactly one Datasets-typed parameter (typically
    keyed ``datasetsSearch``). This helper extracts it into a structured
    block the LLM and the ``add_module_to_tab`` tool can both consume::

        {
          "settings_key": "datasetsSearch",   # which key in `settings`
          "required": True,                   # is the dataset required?
          "arity": "single" | "multiple",     # parameters.multiple
          "dataset_type": "INTENSITY",        # parameters.type, if declared
          "tool_args": {                      # which add_module_to_tab args
            "ids":      "dataset_id" or "dataset_ids",
            "uploads":  "upload_id"  or "upload_ids",
          },
        }

    Returns None when the module has no Datasets field (e.g. ``heading``,
    ``page_break``, ``text``).
    """
    schema = module.input_settings
    if not schema:
        return None

    # Iterate parameters in declaration order; pick the first Datasets field.
    if isinstance(schema, dict):
        items = list(schema.items())
    else:
        items = [(str(s.get("key")), s) for s in schema if isinstance(s, dict)]

    for key, spec in items:
        if not isinstance(spec, dict):
            continue
        field_type = spec.get("fieldType") or spec.get("type")
        if field_type != "Datasets":
            continue
        params = spec.get("parameters") or {}
        is_multiple = bool(params.get("multiple", False))
        return {
            "settings_key": key,
            "required": _is_required(spec),
            "arity": "multiple" if is_multiple else "single",
            "dataset_type": params.get("type"),
            "tool_args": {
                "ids": "dataset_ids" if is_multiple else "dataset_id",
                "uploads": "upload_ids" if is_multiple else "upload_id",
            },
        }
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Field-type-level fallback defaults.
#
# Some registry parameters are required but declare ``default: null`` (or omit
# the default entirely). The React renderer / instruction layer in the
# ``workflow`` repo picks a concrete shape for these at render time — we
# mirror those choices here so add_module_to_tab can build a complete
# settings hash that the renderer will accept without prompting.
#
# Source-of-truth: workflow/app/javascript/workspaces/lib/instructions/*.js
# (e.g. ModuleCVDistributionPlot xAxis falls back to
# [{field: 'sample_name', order: 'none'}] in the OrderableSampleMetadataColumns
# instruction type).
#
# This is best-effort. When the renderer needs something we cannot guess
# (e.g. a specific protein list id), the LLM still has to supply it and we
# fall through to ``RegisteredModule.missing_required_keys`` which fails fast.
# ──────────────────────────────────────────────────────────────────────────────
_FIELD_TYPE_FALLBACKS: Dict[str, Any] = {
    "OrderableSampleMetadataColumns": [{"field": "sample_name", "order": "none"}],
    "SampleMetadataValuesFilter": {"values": []},
    # DatasetSampleMetadataValues stores a FLAT list, not a {values: [...]}
    # envelope (that's SampleMetadataValuesFilter's shape). Every workflow
    # instruction using this field type declares ``default: []`` and the Vue
    # field emits ``Array.from(this.selectedValues)`` on change
    # (workflow/app/javascript/workspaces/lib/fields/DatasetSampleMetadataValuesField.vue:62).
    # Wrapping it as {"values": []} broke the Dimensionality Reduction
    # render — vis-service's request validator surfaced
    # "Invalid sample_names type: <class 'dict'>. Expected List[Union[int, str]]"
    # (visualisations-service/src/requests/dimensionality_reduction_request.py:237).
    "DatasetSampleMetadataValues": [],
    "ProteinLists": [],
    "ProteinSelection": {
        "proteinListId": None,
        "proteinListData": None,
        "proteins": [],
    },
}


def field_type_fallbacks(module: RegisteredModule) -> Dict[str, Any]:
    """Hidden-default overlay for fields that are required-no-registry-default
    but for which the JS instruction layer picks a concrete shape.

    Returns ``{settings_key: fallback_value}`` for each field in the
    module's input_settings whose fieldType has a known fallback AND whose
    registry default is null / missing. The MCP merges this overlay below
    user-supplied settings, so the LLM can still override.
    """
    schema = module.input_settings
    if not schema:
        return {}
    if isinstance(schema, dict):
        items = list(schema.items())
    else:
        items = [
            (str(s.get("key")), s)
            for s in schema
            if isinstance(s, dict) and s.get("key") is not None
        ]
    out: Dict[str, Any] = {}
    for key, spec in items:
        if not isinstance(spec, dict):
            continue
        if spec.get("default") is not None:
            continue  # registry already supplies a default; don't override
        field_type = spec.get("fieldType") or spec.get("type")
        if field_type in _FIELD_TYPE_FALLBACKS:
            out[str(key)] = _FIELD_TYPE_FALLBACKS[field_type]
    return out


def build_dataset_envelope(
    *,
    dataset_id: str,
    dataset_name: str,
    upload_id: str,
    dataset_type: Optional[str],
    keywords: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Build a single-entry datasetsSearch envelope.

    Mirrors the shape persisted by the app for a freshly-selected dataset
    (probed live from existing modules on dev — see PR notes). For
    ``multiple: true`` modules, callers compose a list of these wrapped in
    ``individualResults`` (see ``build_dataset_envelope_multi``).

    The envelope's ``experimentId`` is the upload's UUID; Mass Dynamics
    has historically conflated "experiment" and "upload" in this surface.
    """
    return {
        "type": dataset_type,
        "searchResult": None,
        "individualResults": [
            {
                "id": dataset_id,
                "name": dataset_name,
                "experimentId": upload_id,
            }
        ],
        "liveUpdate": False,
        "keywords": list(keywords) if keywords else [dataset_name],
    }


def build_dataset_envelope_multi(
    entries: Sequence[Dict[str, str]],
    dataset_type: Optional[str],
) -> Dict[str, Any]:
    """Build a multi-entry datasetsSearch envelope.

    ``entries`` is a list of dicts with keys ``id``, ``name``, ``upload_id``
    (one per dataset). Used for ``multiple: true`` modules
    (e.g. ``dose_response_curve_plot``).
    """
    individual = [
        {
            "id": e["id"],
            "name": e["name"],
            "experimentId": e["upload_id"],
        }
        for e in entries
    ]
    return {
        "type": dataset_type,
        "searchResult": None,
        "individualResults": individual,
        "liveUpdate": False,
        "keywords": [e["name"] for e in entries],
    }
