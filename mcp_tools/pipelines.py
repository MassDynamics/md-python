import json
from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import (
    DoseResponseDataset,
    NormalisationImputationDataset,
    PairwiseComparisonDataset,
)
from md_python.models.metadata import SampleMetadata

from . import mcp
from ._client import get_client

# ---------------------------------------------------------------------------
# Parameter schemas — single source of truth for every pipeline type.
# Update here when the API adds new methods or options.
# ---------------------------------------------------------------------------
_PIPELINE_SCHEMAS: Dict[str, Any] = {
    "normalisation_imputation": {
        "description": "Normalise and impute missing values in an intensity dataset.",
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "normalisation_method",
            "imputation_method",
        ],
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "normalisation_method": {
                "type": "str",
                "valid_values": ["median", "quantile"],
                "description": "Normalisation algorithm to apply.",
            },
            "imputation_method": {
                "type": "str",
                "valid_values": ["min_value", "knn"],
                "description": "Imputation algorithm to apply.",
            },
            "normalisation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": "Extra kwargs merged into the normalisation method dict (optional).",
            },
            "imputation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": "Extra kwargs merged into the imputation method dict (optional). E.g. {'k': 5} for knn.",
            },
        },
    },
    "dose_response": {
        "description": "Fit dose-response curves to intensity data.",
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_names",
            "control_samples",
            "sample_metadata",
            "dose_column",
        ],
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "sample_names": {
                "type": "List[str]",
                "description": "All sample names included in the analysis. Must match sample_name values in sample_metadata exactly.",
            },
            "control_samples": {
                "type": "List[str]",
                "description": "Subset of sample_names used as controls (dose = 0).",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and dose_column. Dose values are converted to numbers.",
            },
            "dose_column": {
                "type": "str",
                "default": "dose",
                "description": "Column in sample_metadata containing dose values.",
            },
            "log_intensities": {
                "type": "bool",
                "default": True,
                "description": "Log-transform intensities before fitting.",
            },
            "use_imputed_intensities": {
                "type": "bool",
                "default": True,
                "description": "Use imputed intensity values.",
            },
            "normalise": {
                "type": "str",
                "default": "none",
                "valid_values": ["none"],
                "description": "Normalisation to apply before fitting. Use 'none' (no normalisation is the standard choice).",
            },
            "span_rollmean_k": {
                "type": "int",
                "default": 1,
                "description": "Rolling mean window size (>= 1). Use 1 to disable smoothing.",
            },
            "prop_required_in_protein": {
                "type": "float",
                "default": 0.5,
                "description": "Minimum fraction of non-missing values required per protein [0, 1].",
            },
        },
    },
    "pairwise_comparison": {
        "description": "Run limma-based pairwise differential expression analysis.",
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_metadata",
            "condition_column",
            "condition_comparisons",
        ],
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and condition_column.",
            },
            "condition_column": {
                "type": "str",
                "description": "Column in sample_metadata defining groups to compare (e.g. 'condition').",
            },
            "condition_comparisons": {
                "type": "List[List[str]]",
                "description": "List of [case, control] pairs. Use generate_pairwise_comparisons to build these.",
            },
            "filter_valid_values_logic": {
                "type": "str",
                "default": "at least one condition",
                "valid_values": [
                    "all conditions",
                    "at least one condition",
                    "full experiment",
                ],
                "description": "Controls which rows pass the valid-value filter.",
            },
            "filter_method": {
                "type": "str",
                "default": "percentage",
                "valid_values": ["percentage", "count"],
                "description": "Method for the valid-value filter.",
            },
            "filter_threshold_percentage": {
                "type": "float",
                "default": 0.5,
                "description": "Fraction [0, 1] of valid values required (used when filter_method='percentage').",
            },
            "fit_separate_models": {
                "type": "bool",
                "default": True,
                "description": "Fit a separate limma model per comparison.",
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": "Apply limma trend (intensity-dependent prior variance).",
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": "Apply robust empirical Bayes moderation.",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "peptide"],
                "description": "Entity level to analyse.",
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": "Covariates to include in the model. Each item: {'column': str, 'type': 'numerical'|'categorical'}.",
            },
        },
    },
}


@mcp.tool()
def describe_pipeline(job_slug: str) -> str:
    """Return the full parameter schema for a pipeline before running it.

    ALWAYS call this before run_normalisation_imputation, run_dose_response, or
    run_pairwise_comparison. It lists every accepted parameter, its type, default
    value, and — crucially — the exact valid_values the API accepts. Never guess
    parameter names or values; use only what this tool returns.

    job_slug: one of "normalisation_imputation", "dose_response", "pairwise_comparison".
    Use list_jobs() to see all available slugs.
    """
    schema = _PIPELINE_SCHEMAS.get(job_slug)
    if schema is None:
        available = ", ".join(sorted(_PIPELINE_SCHEMAS))
        return f"Unknown job_slug '{job_slug}'. Known slugs with schemas: {available}"
    return json.dumps(schema, indent=2)


@mcp.tool()
def run_normalisation_imputation(
    input_dataset_ids: List[str],
    dataset_name: str,
    normalisation_method: str,
    imputation_method: str,
    normalisation_extra_params: Optional[Dict[str, Any]] = None,
    imputation_extra_params: Optional[Dict[str, Any]] = None,
) -> str:
    """Run a normalisation + imputation pipeline.

    BEFORE calling this tool, call describe_pipeline("normalisation_imputation") to
    confirm valid parameter values. Do NOT guess method names.

    Returns the new dataset ID on success.
    """
    norm: Dict[str, Any] = {"method": normalisation_method}
    if normalisation_extra_params:
        norm.update(normalisation_extra_params)

    imp: Dict[str, Any] = {"method": imputation_method}
    if imputation_extra_params:
        imp.update(imputation_extra_params)

    dataset_id = NormalisationImputationDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        normalisation_methods=norm,
        imputation_methods=imp,
    ).run(get_client())
    return f"Normalisation/imputation pipeline started. Dataset ID: {dataset_id}"


@mcp.tool()
def generate_pairwise_comparisons(
    sample_metadata: List[List[str]],
    condition_column: str,
    control: Optional[str] = None,
) -> str:
    """Generate pairwise comparison pairs from sample metadata.

    If control is provided: generates all [case, control] pairs vs that one control.
    If control is omitted: generates all unique pairwise combinations.

    Returns a JSON list of [case, control] pairs to pass to run_pairwise_comparison.
    """
    sm = SampleMetadata(data=sample_metadata)
    if control:
        pairs = PairwiseComparisonDataset.pairwise_vs_control(
            sm, condition_column, control
        )
    else:
        pairs = PairwiseComparisonDataset.all_pairwise_comparisons(sm, condition_column)
    return json.dumps(pairs, indent=2)


@mcp.tool()
def run_pairwise_comparison(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: List[List[str]],
    condition_column: str,
    condition_comparisons: List[List[str]],
    filter_valid_values_logic: str = "at least one condition",
    filter_method: str = "percentage",
    filter_threshold_percentage: float = 0.5,
    fit_separate_models: bool = True,
    limma_trend: bool = True,
    robust_empirical_bayes: bool = True,
    entity_type: str = "protein",
    control_variables: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Run a pairwise comparison (limma) pipeline.

    BEFORE calling this tool, call describe_pipeline("pairwise_comparison") to confirm
    valid parameter names and values. Do NOT guess or invent parameter values.

    Use generate_pairwise_comparisons to build condition_comparisons from sample metadata.

    Returns the new dataset ID on success.
    """
    cv: Optional[Dict[str, List[Dict[str, str]]]] = (
        {"control_variables": control_variables} if control_variables else None
    )

    dataset_id = PairwiseComparisonDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        sample_metadata=SampleMetadata(data=sample_metadata),
        condition_column=condition_column,
        condition_comparisons=condition_comparisons,
        filter_valid_values_logic=filter_valid_values_logic,
        filter_values_criteria={
            "method": filter_method,
            "filter_threshold_percentage": filter_threshold_percentage,
        },
        fit_separate_models=fit_separate_models,
        limma_trend=limma_trend,
        robust_empirical_bayes=robust_empirical_bayes,
        entity_type=entity_type,
        control_variables=cv,
    ).run(get_client())
    return f"Pairwise comparison pipeline started. Dataset ID: {dataset_id}"


@mcp.tool()
def run_dose_response(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_names: List[str],
    control_samples: List[str],
    sample_metadata: Optional[List[List[str]]] = None,
    dose_column: str = "dose",
    log_intensities: bool = True,
    use_imputed_intensities: bool = True,
    normalise: str = "none",
    span_rollmean_k: int = 1,
    prop_required_in_protein: float = 0.5,
) -> str:
    """Run a dose-response curve fitting pipeline.

    BEFORE calling this tool, call describe_pipeline("dose_response") to confirm
    valid parameter names and values. Do NOT guess or invent parameter values.

    Returns the new dataset ID on success.
    """
    dataset_id = DoseResponseDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        sample_names=sample_names,
        control_samples=control_samples,
        sample_metadata=(
            SampleMetadata(data=sample_metadata) if sample_metadata else None
        ),
        dose_column=dose_column,
        log_intensities=log_intensities,
        use_imputed_intensities=use_imputed_intensities,
        normalise=normalise,
        span_rollmean_k=span_rollmean_k,
        prop_required_in_protein=prop_required_in_protein,
    ).run(get_client())
    return f"Dose-response pipeline started. Dataset ID: {dataset_id}"
