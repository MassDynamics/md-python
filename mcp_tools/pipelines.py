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

    normalisation_method: method name (e.g. "median", "quantile").
    imputation_method: method name (e.g. "min_value", "knn").
    normalisation_extra_params / imputation_extra_params: optional additional parameters
    merged into the respective method dicts.

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

    sample_metadata: 2D array — first row is header (must include sample_name and
    condition_column), subsequent rows are data.

    condition_comparisons: list of [case, control] pairs,
    e.g. [["treated", "control"]]. Use generate_pairwise_comparisons to build these.

    filter_valid_values_logic: one of "all conditions", "at least one condition",
    "full experiment".
    filter_method: "percentage" or "count".
    filter_threshold_percentage: fraction in [0, 1] (used when filter_method="percentage").

    control_variables: optional list of {"column": str, "type": "numerical"|"categorical"}.
    entity_type: "protein" or "peptide".

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

    sample_names: all sample names included in the analysis.
    control_samples: subset of sample_names used as controls.
    sample_metadata: optional 2D array with dose information
    (first row header, e.g. [sample_name, dose]).

    dose_column: column in sample_metadata containing dose values (default "dose").
    log_intensities: log-transform intensities (default True).
    use_imputed_intensities: use imputed values (default True).
    normalise: normalisation method (default "none").
    span_rollmean_k: rolling mean span >= 1 (default 1).
    prop_required_in_protein: fraction [0,1] required per protein (default 0.5).

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
