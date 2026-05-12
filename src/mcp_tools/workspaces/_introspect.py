"""Module-registry parameter introspection.

The live registry payload (``GET /module_registry/modules/:id``) is rich but
opaque — every parameter carries a ``fieldType`` (Datasets, EntityType,
DatasetSampleMetadata, ProteinList, …), optional ``when`` conditional-
visibility clauses, and cross-field ``ref`` parameters. This module turns
that payload into a structured per-parameter doc tuned for the LLM:

  * Every parameter is listed — even when ``default`` is ``null`` or the
    field is optional.
  * Every parameter says **what kind of value it expects** in plain language
    (e.g. "an INTENSITY dataset id", "a sample-metadata column name from
    the chosen dataset", "a list of protein-list ids of the right entity
    type").
  * Every parameter declares its **data dependencies** — what the LLM has
    to know about (sample metadata, entity lists, dataset type) BEFORE it
    can fill the value sensibly.
  * Conditional parameters carry a ``condition`` block — the LLM should
    only set the parameter when the condition holds.
  * Cross-field references are resolved into human-readable text — e.g.
    "entityType depends on the dataset chosen in datasetsSearch".

The output is the source-of-truth that ``describe_module_type`` returns to
the LLM and is what the parameter Q&A mandate is grounded against.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from md_python.models import RegisteredModule

# ──────────────────────────────────────────────────────────────────────────────
# Field-type → semantic profile.
#
# Every fieldType the registry uses is mapped to:
#   * value_kind         — short human label ("dataset_id", "string-enum", …).
#   * value_description  — sentence the LLM can quote to the user.
#   * data_dependencies  — what the LLM must already know about. The LLM is
#                          expected to fetch / confirm these before suggesting
#                          a value.
#   * fillable_by_llm    — True for primitives + enums; False when the value
#                          must come from data (dataset id, sample metadata,
#                          entity list). The LLM may *propose* a value for
#                          False fields but must confirm with the user.
#
# When a fieldType is missing from this map the introspection still runs —
# the unknown profile is returned with ``value_kind="unknown"`` so the LLM
# at least knows the gap exists.
# ──────────────────────────────────────────────────────────────────────────────
_FIELD_TYPE_PROFILES: Dict[str, Dict[str, Any]] = {
    "Datasets": {
        "value_kind": "dataset_id(s)",
        "value_description": (
            "One or more Mass Dynamics dataset ids. The dataset(s) must be "
            "the type required by parameters.type (e.g. INTENSITY, PAIRWISE, "
            "DOSE_RESPONSE, ANOVA). multiple=true means a list of ids; "
            "multiple=false means a single id."
        ),
        "data_dependencies": [
            "dataset_id of the appropriate type for this module",
        ],
        "fillable_by_llm": False,
    },
    "EntityType": {
        "value_kind": "entity_type",
        "value_description": (
            "The entity type for the chosen dataset — protein, peptide, or "
            "gene. The valid choice depends on what was uploaded; resolve "
            "via the dataset referenced by parameters.datasetsSearch.ref."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the dataset's entity_type — get it via list_datasets / "
            "find_initial_dataset → Dataset.type, OR by inspecting the "
            "upload source (md_format_gene → gene; md_format → protein/"
            "peptide; etc.)",
        ],
        "fillable_by_llm": False,
    },
    "ProteinList": {
        "value_kind": "entity_list_id (single)",
        "value_description": (
            "Optional id of a single entity list (protein / peptide / gene "
            "list). The list's entity type must match parameters.type "
            "(usually a ref to entityType)."
        ),
        "data_dependencies": [
            "entity-list id from the user's organisation — listed in the "
            "Mass Dynamics app under 'Lists' (no MCP endpoint surfaces them "
            "today; ask the user)",
        ],
        "fillable_by_llm": False,
    },
    "ProteinLists": {
        "value_kind": "entity_list_id(s) — list",
        "value_description": (
            "Optional list of entity-list ids. Multiple lists are unioned in "
            "the rendered plot. enableSettings=true means each list also "
            "carries its own visual customisation."
        ),
        "data_dependencies": [
            "zero or more entity-list ids from the user's organisation",
        ],
        "fillable_by_llm": False,
    },
    "ProteinSelection": {
        "value_kind": "protein-selection envelope",
        "value_description": (
            "Specific protein selection. Shape: "
            "{proteinListId, proteinListData, proteins}. Use proteins=[...] "
            "for an explicit list of protein-group ids; use proteinListId "
            "to reference a saved list. Empty default selects nothing."
        ),
        "data_dependencies": [
            "protein-group ids (e.g. from query_entities) OR an entity-list "
            "id from the user's organisation",
        ],
        "fillable_by_llm": False,
    },
    "DatasetSampleMetadata": {
        "value_kind": "sample-metadata column name",
        "value_description": (
            "A single column name from the chosen dataset's sample metadata "
            "(e.g. 'condition', 'batch', 'treatment'). The valid set "
            "depends on the upload's metadata; fetch it via "
            "get_upload_sample_metadata BEFORE suggesting a value."
        ),
        "data_dependencies": [
            "sample_metadata for the upload that produced this dataset — "
            "call get_upload_sample_metadata(upload_id) first; the available "
            "column names are the keys of that 2D array's header row",
        ],
        "fillable_by_llm": False,
    },
    "DatasetSampleMetadataValues": {
        "value_kind": "sample-metadata value list (whitelist)",
        "value_description": (
            "List of values from a specific sample-metadata column "
            "(parameters.columnName). Acts as a whitelist — only matching "
            "samples are used. Empty list means 'use all samples'."
        ),
        "data_dependencies": [
            "sample_metadata for the upload (call get_upload_sample_metadata)",
            "the values present under parameters.columnName — usually "
            "'sample_name', sometimes 'condition' or another field",
        ],
        "fillable_by_llm": False,
    },
    "OrderableSampleMetadataColumns": {
        "value_kind": "ordered list of metadata columns",
        "value_description": (
            "Ordered list of {field, order} dicts where field is a "
            "sample-metadata column name and order is one of "
            "'asc', 'desc', or 'none'. The order in the list is the "
            "grouping hierarchy on the X-axis."
        ),
        "data_dependencies": [
            "sample_metadata column names for the upload",
        ],
        "fillable_by_llm": False,
    },
    "SampleMetadataValuesFilter": {
        "value_kind": "sample-metadata filter envelope",
        "value_description": (
            "Filter spec: {values: [...]} where values are entries from the "
            "column named in parameters.columnName. Same as "
            "DatasetSampleMetadataValues but wrapped in a filter envelope "
            "with optional advanced filtering."
        ),
        "data_dependencies": [
            "sample_metadata for the upload",
            "values present under parameters.columnName",
        ],
        "fillable_by_llm": False,
    },
    "ConditionComparison": {
        "value_kind": "{experimentId, conditionPair} envelope",
        "value_description": (
            "A specific case-vs-control pair from a PAIRWISE-typed "
            "dataset. The valid pairs are stored on the dataset itself "
            "(dataset.job_run_params.condition_comparisons) and depend on "
            "what the user ran in run_pairwise_comparison."
        ),
        "data_dependencies": [
            "the PAIRWISE dataset chosen in datasetsSearch",
            "the comparisons computed when the user ran the pairwise "
            "pipeline — visible on Dataset.job_run_params.condition_comparisons",
        ],
        "fillable_by_llm": False,
    },
    "ColourPalette": {
        "value_kind": "palette id",
        "value_description": (
            "Optional colour-palette identifier. No defaults — when omitted "
            "the app uses its current palette."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "RadioSelectionField": {
        "value_kind": "string-enum (radio)",
        "value_description": (
            "One of the values in parameters.options[].value. The labels in "
            "parameters.options[].name are user-facing only — always send "
            "the value, not the name."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "String": {
        "value_kind": "string-or-enum",
        "value_description": (
            "Free-form string when parameters.options is absent; otherwise "
            "an enum — pick one parameters.options[].value (NOT the name)."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "Number": {
        "value_kind": "number",
        "value_description": (
            "Numeric value. Bounds (if any) are encoded in parameters."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "Boolean": {
        "value_kind": "boolean",
        "value_description": "true or false.",
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
}


_NULL_DEFAULT_NOTE = (
    "default is explicitly null — the API persists null and the rendered "
    "widget shows 'Please provide ...' until the LLM/user supplies a value"
)


_MISSING_DEFAULT_NOTE = (
    "no default declared — leaving this unset means the API persists "
    "nothing for this key and the rendered widget will surface a "
    "'Please provide ...' prompt"
)


def _profile_for(field_type: Optional[str]) -> Dict[str, Any]:
    if field_type is None:
        return {
            "value_kind": "unknown",
            "value_description": (
                "fieldType missing from the registry payload — treat as "
                "opaque and consult the module's prose description."
            ),
            "data_dependencies": [],
            "fillable_by_llm": False,
        }
    profile = _FIELD_TYPE_PROFILES.get(field_type)
    if profile is None:
        return {
            "value_kind": f"unmapped:{field_type}",
            "value_description": (
                f"fieldType {field_type!r} is not yet mapped in the MCP "
                "introspection helper. Treat as opaque and consult the "
                "module's prose description; flag this to the maintainers."
            ),
            "data_dependencies": [],
            "fillable_by_llm": False,
        }
    return dict(profile)


def _is_required(spec: Dict[str, Any]) -> bool:
    if spec.get("required") is True:
        return True
    rules = spec.get("rules")
    if isinstance(rules, list):
        for rule in rules:
            if isinstance(rule, dict) and rule.get("name") == "is_required":
                return True
    return False


def _resolve_refs(parameters: Optional[Dict[str, Any]]) -> List[str]:
    """Surface cross-field {ref: '<other_key>'} dependencies as plain text."""
    if not isinstance(parameters, dict):
        return []
    refs: List[str] = []
    for k, v in parameters.items():
        if isinstance(v, dict) and "ref" in v:
            refs.append(
                f"this field's value-space comes from the field {v['ref']!r} "
                f"(parameters.{k}.ref); resolve {v['ref']!r} first"
            )
    return refs


def _options_summary(
    parameters: Optional[Dict[str, Any]],
) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(parameters, dict):
        return None
    opts = parameters.get("options")
    if not isinstance(opts, list):
        return None
    out: List[Dict[str, Any]] = []
    for opt in opts:
        if isinstance(opt, dict):
            out.append({"value": opt.get("value"), "label": opt.get("name")})
    return out or None


def _condition_text(spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Turn a `when` clause into a human-readable {field, predicate, value}."""
    when = spec.get("when")
    if not isinstance(when, dict):
        return None
    if "equals" in when:
        return {
            "depends_on": when.get("property"),
            "predicate": "equals",
            "value": when["equals"],
            "human": (
                f"only set when {when.get('property')!r} == " f"{when['equals']!r}"
            ),
        }
    if "not_equals" in when:
        return {
            "depends_on": when.get("property"),
            "predicate": "not_equals",
            "value": when["not_equals"],
            "human": (
                f"only set when {when.get('property')!r} != " f"{when['not_equals']!r}"
            ),
        }
    return {
        "depends_on": when.get("property"),
        "predicate": "custom",
        "raw": when,
        "human": f"only set when condition is met (raw: {when})",
    }


def _default_note(spec: Dict[str, Any]) -> Optional[str]:
    if "default" in spec:
        if spec["default"] is None:
            return _NULL_DEFAULT_NOTE
        return None
    return _MISSING_DEFAULT_NOTE


def parameter_doc(key: str, spec: Any) -> Dict[str, Any]:
    """Fully-populated doc for one input_settings entry.

    Always returns every key documented in the schema below, even when the
    value is None — the LLM is expected to read this verbatim and present
    it to the user without skipping rows.
    """
    if not isinstance(spec, dict):
        # Some dict-shape entries can be primitives (e.g. raw default-only
        # specs); represent them as opaque-but-documented rows.
        return {
            "key": key,
            "name": key,
            "group": None,
            "field_type": None,
            "value_kind": "literal",
            "value_description": (
                "Spec is a literal value — no schema. Treat as opaque; the "
                "literal IS the platform default."
            ),
            "platform_default": spec,
            "default_present": spec is not None,
            "default_note": None,
            "is_required": False,
            "data_dependencies": [],
            "cross_field_refs": [],
            "options": None,
            "condition": None,
            "description": None,
            "fillable_by_llm": False,
            "raw_parameters": None,
        }

    field_type = spec.get("fieldType") or spec.get("type")
    profile = _profile_for(field_type)
    refs = _resolve_refs(spec.get("parameters"))

    return {
        "key": key,
        "name": spec.get("name", key),
        "group": spec.get("group"),
        "field_type": field_type,
        "value_kind": profile["value_kind"],
        "value_description": profile["value_description"],
        "platform_default": spec.get("default") if "default" in spec else None,
        "default_present": "default" in spec and spec.get("default") is not None,
        "default_note": _default_note(spec),
        "is_required": _is_required(spec),
        "data_dependencies": profile["data_dependencies"],
        "cross_field_refs": refs,
        "options": _options_summary(spec.get("parameters")),
        "condition": _condition_text(spec),
        "description": spec.get("description"),
        "fillable_by_llm": profile["fillable_by_llm"],
        "raw_parameters": spec.get("parameters"),
    }


def parameters_for(module: RegisteredModule) -> List[Dict[str, Any]]:
    """List of parameter docs in declaration order.

    Handles both wire encodings:
      * dict shape — {key: spec}; iteration order preserved (Python 3.7+).
      * array shape — list of {key, ...}; preserves array order.
    """
    schema = module.input_settings
    if not schema:
        return []
    if isinstance(schema, dict):
        return [parameter_doc(str(k), v) for k, v in schema.items()]
    out: List[Dict[str, Any]] = []
    for entry in schema:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        if key is None:
            continue
        out.append(parameter_doc(str(key), entry))
    return out


def _aggregate_data_dependencies(
    params: Sequence[Dict[str, Any]],
) -> List[str]:
    seen: List[str] = []
    for p in params:
        for dep in p.get("data_dependencies") or []:
            if dep not in seen:
                seen.append(dep)
    return seen


def entity_type_input_for(module: RegisteredModule) -> Optional[Dict[str, Any]]:
    """Find the EntityType-typed parameter on a module, if any.

    Most plot modules carry an ``entityType`` field that says whether the
    dataset is being interpreted as protein, peptide, or gene level. This
    is required-no-default for almost every plot — the LLM has to supply
    it. Returns the structured block for the tool layer or None when the
    module has no EntityType field.
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
            "valid_values": ["protein", "peptide", "gene"],
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
    "DatasetSampleMetadataValues": {"values": []},
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
