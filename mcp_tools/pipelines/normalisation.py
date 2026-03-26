"""Normalisation + imputation pipeline tools."""

from functools import partial
from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import NormalisationImputationDataset

from .. import mcp
from .._client import get_client
from ._bulk import _MAX_BULK_JOBS, _bulk_prefetch_upload_data, _run_jobs_parallel


@mcp.tool()
def run_normalisation_imputation(
    input_dataset_ids: List[str],
    dataset_name: str,
    normalisation_method: str,
    imputation_method: str,
    entity_type: str = "protein",
    normalisation_extra_params: Optional[Dict[str, Any]] = None,
    imputation_extra_params: Optional[Dict[str, Any]] = None,
) -> str:
    """Run a normalisation + imputation pipeline.

    ALWAYS ask the user which normalisation and imputation methods to use before
    calling this tool, unless the user has explicitly asked you to suggest the
    best option based on their data type. Do not silently pick defaults.

    entity_type: "protein" (default), "peptide", or "gene". Must match the data type
      in the upstream intensity dataset.

    Valid normalisation_method values:
      "median"           — robust, recommended for most proteomics experiments
      "quantile"         — stronger normalisation, assumes similar distributions
      "none"             — skip normalisation (use if data is already normalised)
      "batch_correction" — correct for batch effects; requires batch_variables and
                           design_variables in normalisation_extra_params

    Valid imputation_method values:
      "mnar"             — PREFERRED for proteomics: left-tail Gaussian draw for
                           Missing Not At Random data. Accepts optional extra params:
                           std_position (default 1.8) and std_width (default 0.3).
      "knn"              — K-nearest neighbours; good for MAR data. Accepts k in
                           imputation_extra_params (e.g. {"k": 5}).
      "global_median"    — replace missing with the global median intensity
      "median_by_entity" — replace missing with per-protein/gene median

    For standard DDA proteomics, "mnar" is the recommended imputation method
    because low-abundance proteins are systematically absent (MNAR pattern).

    Call describe_pipeline("normalisation_imputation") for the full schema.
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
        entity_type=entity_type,
    ).run(get_client())
    return f"Normalisation/imputation pipeline started. Dataset ID: {dataset_id}"


def _submit_ni_job(
    i: int,
    job: Dict[str, Any],
    existing_cache: Dict[str, Dict[str, str]],
    initial_ds_cache: Dict[str, Optional[str]],
) -> Dict[str, Any]:
    """Process one normalisation job for run_normalisation_imputation_bulk."""
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

    initial_id = initial_ds_cache.get(upload_id)
    if not initial_id:
        entry["error"] = f"No initial INTENSITY dataset found for upload {upload_id}"
        entry["error_code"] = "dataset_not_found"
        return entry

    try:
        run_result = run_normalisation_imputation(
            input_dataset_ids=[initial_id],
            dataset_name=dataset_name,
            normalisation_method=job.get("normalisation_method", ""),
            imputation_method=job.get("imputation_method", ""),
            entity_type=job.get("entity_type", "protein"),
            normalisation_extra_params=job.get("normalisation_extra_params"),
            imputation_extra_params=job.get("imputation_extra_params"),
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
def run_normalisation_imputation_bulk(jobs: List[Dict[str, Any]]) -> str:
    """Submit multiple normalisation + imputation jobs in a single call (max 500).

    Resolves input_dataset_ids from upload_id automatically, with per-upload
    prefetching so list_by_upload is called at most once per unique upload.
    Jobs are submitted in parallel (up to 20 concurrent connections).
    All jobs are attempted regardless of individual failures.

    Default if_exists="skip" skips uploads that already have a
    NORMALISATION_IMPUTATION dataset with the same name.

    Each job spec (dict):
      upload_id                str   — upload to run against (required)
      dataset_name             str   — name for the output dataset (required)
      normalisation_method     str   — "median" or "quantile" (required)
      imputation_method        str   — "min_value" or "knn" (required)
      normalisation_extra_params dict — extra kwargs for normalisation (optional)
      imputation_extra_params    dict — extra kwargs for imputation (optional)
      if_exists                str   — "skip" (default) or "run"

    Returns JSON array:
      [{index, upload_id, dataset_name, dataset_id?, skipped?, error?, error_code?}]
    Or a JSON error object if len(jobs) > 500.
    """
    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}
    initial_ds_cache: Dict[str, Optional[str]] = {}

    for uid in unique_ids:
        intensity_id, existing = _bulk_prefetch_upload_data(
            uid, "NORMALISATION_IMPUTATION"
        )
        existing_cache[uid] = existing
        initial_ds_cache[uid] = intensity_id

    process_fn = partial(
        _submit_ni_job,
        existing_cache=existing_cache,
        initial_ds_cache=initial_ds_cache,
    )
    return _run_jobs_parallel(jobs, process_fn)
