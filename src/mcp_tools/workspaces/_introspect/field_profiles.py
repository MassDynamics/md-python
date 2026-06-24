"""Field-type → semantic profile lookup table.

Every fieldType the registry uses is mapped to:
  * value_kind         — short human label ("dataset_id", "string-enum", …).
  * value_description  — sentence the LLM can quote to the user.
  * data_dependencies  — what the LLM must already know about. The LLM is
                         expected to fetch / confirm these before suggesting
                         a value.
  * fillable_by_llm    — True for primitives + enums; False when the value
                         must come from data (dataset id, sample metadata,
                         entity list). The LLM may *propose* a value for
                         False fields but must confirm with the user.

When a fieldType is missing from this map the introspection still runs —
the unknown profile is returned with ``value_kind="unknown"`` so the LLM
at least knows the gap exists.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

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
            "The entity type for the chosen dataset — protein, peptide, "
            "gene, or metabolite. The valid choice depends on what was "
            "uploaded; resolve via the dataset referenced by "
            "parameters.datasetsSearch.ref."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the dataset's entity_type — get it via list_datasets / "
            "find_initial_dataset → Dataset.type, OR by inspecting the "
            "upload source (md_format_gene → gene; md_format_metabolite → "
            "metabolite; md_format → protein/peptide; etc.)",
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
            "entity-list id from the user's organisation — discover via "
            "list_entity_lists(workspace_id) or create via create_entity_list",
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
            "zero or more entity-list ids — discover via list_entity_lists, "
            "or create via create_entity_list",
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
            "id (discover via list_entity_lists / create via "
            "create_entity_list)",
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
        "value_kind": "{comparison: {conditionPair, left, right}} envelope",
        "value_description": (
            "A specific case-vs-control pair from a PAIRWISE-typed "
            "dataset, plus which side is left/right of the log2 ratio. The "
            "valid pairs are stored on the dataset itself "
            "(dataset.job_run_params.condition_comparisons) and depend on "
            "what the user ran in run_pairwise_comparison. add_module_to_tab "
            "AUTO-RESOLVES this from the chosen dataset — by default the "
            "first comparison, oriented case-vs-control. To pick a "
            "different pair or flip left/right, pass the tool's "
            "``comparison=[left, right]`` argument (NOT settings); positive "
            "log2FC means left is more abundant than right."
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
    "PlotSize": {
        "value_kind": "plot_size",
        "value_description": (
            "Plot dimensions. Shape is {fixed: bool} when "
            "auto-sizing to the grid cell, or {fixed: true, width: <px>, "
            "height: <px>} for an explicit size. Width/height are pixels."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "NumberRange": {
        "value_kind": "number-in-range",
        "value_description": (
            "Numeric value bounded by parameters.min and parameters.max "
            "(inclusive). parameters.integer=true means integers only; "
            "otherwise floats are allowed."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "FoldChangeThreshold": {
        "value_kind": "fold_change_threshold",
        "value_description": (
            "Fold-change cut-off. Interpretation depends on the sibling "
            "axis-selection field: linear ratio when axis='fc' (typical "
            "values 1.5 / 2), log2 fold-change when axis='log2fc' "
            "(typical values 0.585 / 1)."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "NComponents": {
        "value_kind": "component_axes",
        "value_description": (
            "Principal-component axes to plot, shaped as "
            "{xAxis: 'PC1', yAxis: 'PC2', ...}. Valid component names "
            "depend on the chosen dataset and the entity/protein-list "
            "used to compute PCA — resolve via parameters.datasetsSearch."
            "ref and (when present) parameters.proteinListId.ref. The "
            "default {xAxis: 'PC1', yAxis: 'PC2'} is almost always fine; "
            "propose changes but confirm with the user."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the principal components available for that dataset",
        ],
        "fillable_by_llm": False,
    },
    "PairwiseConditionPairs": {
        "value_kind": "condition_pairs",
        "value_description": (
            "Set of [case, control] condition pairs to compare, shaped as "
            "{values: [[case, control], ...]}. Values must be condition "
            "names present in the chosen dataset's sample metadata. "
            "parameters.filterable controls UI search; "
            "parameters.conditionsOrderable controls drag-to-reorder."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the condition column values in that dataset's sample metadata "
            "(call get_upload_sample_metadata)",
        ],
        "fillable_by_llm": False,
    },
    "EntityTitle": {
        "value_kind": "entity_label_column",
        "value_description": (
            "Column to use as the display label for each entity (e.g. "
            "'gene_name', 'protein_accession'). parameters.entityType "
            "constrains valid labels — gene-level labels for "
            "entityType='gene', protein-level for 'protein', etc."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "Experiment": {
        "value_kind": "experiment_id",
        "value_description": (
            "An experiment / upload UUID owned by the current user. Get "
            "it from the user or via query_uploads / get_upload."
        ),
        "data_dependencies": [
            "an experiment / upload id — the LLM must ask the user, or "
            "discover one via query_uploads",
        ],
        "fillable_by_llm": False,
    },
    "Multiple": {
        "value_kind": "multi-select",
        "value_description": (
            "List of selected option values; each value must be one of "
            "parameters.options[].value. Order does not matter; "
            "duplicates are ignored."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "Properties": {
        "value_kind": "properties",
        "value_description": (
            "Module-specific nested object. Treat as opaque — its shape "
            "is documented only in the module's prose description. Do "
            "not invent keys; if no value is supplied, the registry "
            "default is sent verbatim."
        ),
        "data_dependencies": [],
        "fillable_by_llm": False,
    },
    "Species": {
        "value_kind": "species_id",
        "value_description": (
            "Species identifier (NCBI taxonomy id). Common values: "
            "9606 (Human), 10090 (Mouse), 559292 (Yeast), 10029 "
            "(Chinese hamster). The allowed set is server-controlled — "
            "the value is validated when the module is rendered."
        ),
        "data_dependencies": [],
        "fillable_by_llm": True,
    },
    "DatasetTable": {
        "value_kind": "dataset_table_name",
        "value_description": (
            "Name of a table inside the chosen dataset (e.g. 'protein', "
            "'peptide', 'gene'). Allowed names depend on the dataset's "
            "schema — resolve via parameters.datasetsSearch.ref."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the table names available in that dataset",
        ],
        "fillable_by_llm": False,
    },
    "DatasetTableValues": {
        "value_kind": "dataset_table_column_values",
        "value_description": (
            "Column / value selection from the chosen dataset's named "
            "table. Depends on parameters.datasetsSearch.ref and "
            "parameters.tableName.ref. parameters.initiallySelectAll="
            "true means the default is 'all columns selected'."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the chosen table inside that dataset "
            "(referenced in parameters.tableName.ref)",
            "the columns available in that table",
        ],
        "fillable_by_llm": False,
    },
    "DatasetSampleMetadataColumns": {
        "value_kind": "sample_metadata_columns",
        "value_description": (
            "List of sample-metadata column names from the chosen "
            "dataset (e.g. ['condition', 'treatment']). Resolve via "
            "parameters.datasetsSearch.ref → get_upload_sample_metadata."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the dataset's sample_metadata column names "
            "(call get_upload_sample_metadata)",
        ],
        "fillable_by_llm": False,
    },
    "DatasetSampleMetadataValue": {
        "value_kind": "sample_metadata_value",
        "value_description": (
            "A single value from a chosen sample-metadata column (e.g. "
            "one condition name). parameters.columnName.ref points to "
            "the field that selects the column."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the chosen column (referenced in parameters.columnName.ref)",
            "the distinct values in that column " "(call get_upload_sample_metadata)",
        ],
        "fillable_by_llm": False,
    },
    "DatasetSampleMetadataValuesOrder": {
        "value_kind": "ordered_sample_metadata_values",
        "value_description": (
            "Explicit ordering (list) of sample-metadata values for a "
            "chosen column — used when the renderer needs a manual "
            "axis order. parameters.columnName.ref points to the column."
        ),
        "data_dependencies": [
            "the chosen dataset (referenced in parameters.datasetsSearch.ref)",
            "the chosen column (referenced in parameters.columnName.ref)",
            "the distinct values in that column " "(call get_upload_sample_metadata)",
        ],
        "fillable_by_llm": False,
    },
}


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
