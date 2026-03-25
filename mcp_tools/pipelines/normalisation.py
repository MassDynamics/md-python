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
    normalisation_extra_params: Optional[Dict[str, Any]] = None,
    imputation_extra_params: Optional[Dict[str, Any]] = None,
) -> str:
    """Run a normalisation + imputation pipeline.

    Valid normalisation_method values: "median", "quantile".
    Valid imputation_method values: "min_value", "knn".
    Call describe_pipeline("normalisation_imputation") if you need the full schema.

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
