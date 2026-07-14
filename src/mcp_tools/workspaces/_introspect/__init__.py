"""Module-registry parameter introspection.

The live registry payload (``GET /module_registry/modules/:id``) is rich but
opaque — every parameter carries a ``fieldType`` (Datasets, EntityType,
DatasetSampleMetadata, ProteinList, …), optional ``when`` conditional-
visibility clauses, and cross-field ``ref`` parameters. This sub-package
turns that payload into a structured per-parameter doc tuned for the LLM.

Layout:
    field_profiles   — fieldType → semantic profile map
    parameter_docs   — per-parameter render (parameter_doc, parameters_for)
    dataset_inputs   — top-level dataset / entity_type / fallback helpers
    describe         — assembled describe() entry point

The public surface (``parameter_doc``, ``parameters_for``,
``entity_type_input_for``, ``dataset_input_for``, ``build_dataset_envelope``,
``build_dataset_envelope_multi``, ``field_type_fallbacks``, ``describe``,
and the private ``_FIELD_TYPE_PROFILES`` / ``_FIELD_TYPE_FALLBACKS`` dicts
some tests assert against) is re-exported here so the historical
``from mcp_tools.workspaces import _introspect`` continues to work.
"""

from .dataset_inputs import (
    _FIELD_TYPE_FALLBACKS,
    _condition_comparison_pairs,
    build_condition_comparison,
    build_dataset_envelope,
    build_dataset_envelope_multi,
    condition_comparison_input_for,
    dataset_input_for,
    entity_type_from_dataset,
    entity_type_from_upload_source,
    entity_type_input_for,
    field_type_fallbacks,
)
from .describe import describe
from .field_profiles import _FIELD_TYPE_PROFILES, _profile_for
from .parameter_docs import (
    _MISSING_DEFAULT_NOTE,
    _NULL_DEFAULT_NOTE,
    _aggregate_data_dependencies,
    _condition_text,
    _default_note,
    _is_required,
    _options_summary,
    _resolve_refs,
    parameter_doc,
    parameters_for,
)

__all__ = [
    "describe",
    "parameter_doc",
    "parameters_for",
    "entity_type_input_for",
    "entity_type_from_dataset",
    "entity_type_from_upload_source",
    "dataset_input_for",
    "condition_comparison_input_for",
    "build_dataset_envelope",
    "build_dataset_envelope_multi",
    "build_condition_comparison",
    "field_type_fallbacks",
]
