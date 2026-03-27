"""Pairwise comparison pipeline tools."""

import json
from functools import partial
from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import PairwiseComparisonDataset
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client
from ._bulk import _bulk_prefetch_upload_data, _run_jobs_parallel


@mcp.tool()
def generate_pairwise_comparisons(
    sample_metadata: List[List[str]],
    condition_column: str,
    control: Optional[str] = None,
) -> str:
    """Generate pairwise comparison pairs from sample metadata.

    Pass load_metadata_from_csv["sample_metadata"] as sample_metadata.
    NEVER construct sample_metadata by hand.

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
    """Run a pairwise differential abundance analysis using limma.

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter                    Default                  Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    condition_comparisons        (from generate_*)        ALL pairs as one list.
                                                          Do NOT make one call per
                                                          pair — see note below.
    entity_type                  "protein"                "protein" | "peptide" | "gene"
    fit_separate_models          True                     True  = one limma model per
                                                            comparison (recommended)
                                                          False = full contrast matrix
                                                            (all pairs, one model)
    filter_valid_values_logic    "at least one            "at least one condition" |
                                  condition"               "all conditions" |
                                                           "full experiment"
    filter_threshold_percentage  0.5 (50 %)               float 0.0 – 1.0
    limma_trend                  True                     True | False
    robust_empirical_bayes       True                     True | False
    control_variables            None                     list of covariate dicts or None

    Explain each choice in plain language. Only proceed once the user has
    confirmed or explicitly asked you to use the recommended defaults.
    ═══════════════════════════════════════════════════════════════════════════════

    BEFORE calling this tool:
      1. load_metadata_from_csv — read sample_metadata from the user's CSV file.
         NEVER construct sample_metadata manually — sample names must come verbatim.
      2. generate_pairwise_comparisons — build condition_comparisons from metadata.

    condition_comparisons: ALL comparison pairs in ONE list.
      A single run_pairwise_comparison call handles any number of pairs — limma
      models all contrasts jointly, which is required for correct FDR correction.
      Do NOT submit separate calls per pair.
      Example — 6 pairs, ONE call:
        condition_comparisons=[
          ["CKD1","Control"], ["CKD2","Control"], ["CKD3","Control"],
          ["CKD1","CKD2"],   ["CKD1","CKD3"],   ["CKD2","CKD3"],
        ]

    entity_type: "protein" (default), "peptide", or "gene".
      Must match the entity type in the upstream intensity dataset.

    filter_valid_values_logic controls which proteins/peptides/genes pass the
      completeness filter before modelling:
        "at least one condition" (default) — keep rows with enough valid values
          in at least one compared condition. Good for most experiments.
        "all conditions" — require completeness in every compared condition.
          More stringent; reduces false positives but loses more data.
        "full experiment" — require completeness across the entire experiment.
          Most stringent; best for very clean, complete datasets.

    filter_threshold_percentage: fraction of samples in a condition that must have
      valid (non-missing) values to pass the filter. Default 0.5 = 50%.

    fit_separate_models: True (default) = fit one limma model per comparison pair,
      recommended for most analyses. False = fit a single full contrast matrix
      model across all pairs simultaneously — can be preferred when sharing
      variance estimates across many comparisons is appropriate.

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


def _submit_pc_job(
    i: int,
    job: Dict[str, Any],
    existing_cache: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """Process one pairwise comparison job for run_pairwise_comparison_bulk."""
    upload_id = job.get("upload_id", "")
    dataset_name = job.get("dataset_name", "")
    entry: Dict[str, Any] = {
        "index": i,
        "upload_id": upload_id,
        "dataset_name": dataset_name,
    }

    if job.get("if_exists", "skip") == "skip":
        existing_id = existing_cache.get(upload_id, {}).get(dataset_name)
        if existing_id:
            entry["dataset_id"] = existing_id
            entry["skipped"] = True
            return entry

    input_dataset_ids = job.get("input_dataset_ids")
    if not input_dataset_ids:
        entry["error"] = "input_dataset_ids is required for pairwise comparison"
        entry["error_code"] = "missing_input"
        return entry

    try:
        run_result = run_pairwise_comparison(
            input_dataset_ids=input_dataset_ids,
            dataset_name=dataset_name,
            sample_metadata=job["sample_metadata"],
            condition_column=job["condition_column"],
            condition_comparisons=job["condition_comparisons"],
            filter_valid_values_logic=job.get(
                "filter_valid_values_logic", "at least one condition"
            ),
            filter_method=job.get("filter_method", "percentage"),
            filter_threshold_percentage=job.get("filter_threshold_percentage", 0.5),
            fit_separate_models=job.get("fit_separate_models", True),
            limma_trend=job.get("limma_trend", True),
            robust_empirical_bayes=job.get("robust_empirical_bayes", True),
            entity_type=job.get("entity_type", "protein"),
            control_variables=job.get("control_variables"),
        )
        if "Dataset ID:" in run_result:
            entry["dataset_id"] = run_result.split("Dataset ID:")[-1].strip()
        else:
            entry["result"] = run_result
    except Exception as e:
        entry["error"] = str(e)
        entry["error_code"] = "run_failed"

    return entry


@mcp.tool()
def run_pairwise_comparison_bulk(jobs: List[Dict[str, Any]]) -> str:
    """Submit multiple pairwise comparison jobs in a single call (max 500).

    Unlike run_dose_response_bulk, input_dataset_ids must be explicit per job
    (cannot auto-resolve — there may be multiple NI datasets per upload).
    upload_id is still required for the dedup check.

    Jobs are submitted in parallel (up to 20 concurrent connections).
    All jobs are attempted regardless of individual failures.

    Default if_exists="skip" skips uploads that already have a
    PAIRWISE_COMPARISON dataset with the same name.

    Each job spec (dict):
      upload_id               str        — upload the job belongs to (required, for dedup)
      input_dataset_ids       list[str]  — NI output dataset IDs (required)
      dataset_name            str        — name for the output dataset (required)
      sample_metadata         list       — 2D array with header row (required)
      condition_column        str        — column defining groups (required)
      condition_comparisons   list       — [[case, control], ...] pairs (required)
      filter_valid_values_logic str      — default "at least one condition"
      filter_method           str        — default "percentage"
      filter_threshold_percentage float — default 0.5
      fit_separate_models     bool       — default True
      limma_trend             bool       — default True
      robust_empirical_bayes  bool       — default True
      entity_type             str        — default "protein"
      control_variables       list       — optional covariates
      if_exists               str        — "skip" (default) or "run"

    Returns JSON array:
      [{index, upload_id, dataset_name, dataset_id?, skipped?, error?, error_code?}]
    Or a JSON error object if len(jobs) > 500.
    """
    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}

    for uid in unique_ids:
        _, existing = _bulk_prefetch_upload_data(uid, "PAIRWISE_COMPARISON")
        existing_cache[uid] = existing

    process_fn = partial(_submit_pc_job, existing_cache=existing_cache)
    return _run_jobs_parallel(jobs, process_fn)
