"""Dose-response pipeline tools."""

from functools import partial
from typing import Any, Dict, List, Optional, Tuple

from md_python.models.dataset_builders import DoseResponseDataset
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client
from ._bulk import _bulk_prefetch_upload_data, _run_jobs_parallel
from ._metadata import _fetch_upload_sample_metadata, _filter_sample_metadata


def _find_existing_dr_dataset(
    upload_id: str, dataset_name: str
) -> Tuple[Optional[str], Optional[str]]:
    """Return (dataset_id, None) if a DOSE_RESPONSE dataset with dataset_name exists,
    (None, error_str) if the lookup fails, or (None, None) if not found.
    """
    try:
        datasets = get_client().datasets.list_by_upload(upload_id)
        for ds in datasets:
            if ds.type == "DOSE_RESPONSE" and ds.name == dataset_name:
                return str(ds.id), None
        return None, None
    except Exception as e:
        return None, str(e)


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
    """Run a dose-response curve fitting pipeline (4-parameter log-logistic model).

    ALWAYS ask the user which parameters to use (control_samples, dose_column,
    normalise setting) before calling this tool, unless the user has explicitly asked
    you to suggest the best option based on their data.

    PREFER run_dose_response_from_upload for a single upload (auto-resolves dataset ID).
    PREFER run_dose_response_bulk for many uploads at once.

    MINIMUM DATA REQUIREMENTS:
      - At least 3 distinct dose levels in sample_metadata[dose_column]
      - At least 5 total replicates across all doses (3+ per dose recommended)
      - control_samples are the samples at dose = 0 (excluded from curve fitting,
        used to anchor the baseline)
    The pipeline will return an error if these minimums are not met.

    BEFORE calling this tool:
      Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
      NEVER construct sample_metadata, sample_names, or control_samples manually —
      all sample names must be read verbatim from the file to avoid mismatches.
      Call describe_pipeline("dose_response") if you need the full parameter schema.

    sample_metadata: pass load_metadata_from_csv["sample_metadata"] directly.
    sample_names: read from sample_metadata rows, not from filenames or inference.
    control_samples: ask the user which samples are controls; never guess.

    use_imputed_intensities: defaults to True (uses imputed values from a prior
      normalisation_imputation step). Set False to use raw intensities only.

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


@mcp.tool()
def run_dose_response_from_upload(
    upload_id: str,
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
    if_exists: str = "skip",
) -> str:
    """Run a dose-response pipeline directly from an upload ID.

    Resolves input_dataset_ids automatically — no need to call find_initial_dataset first.
    PREFERRED over run_dose_response for a single DR job per upload.
    For many DR jobs at once, use run_dose_response_bulk instead.

    Args:
        upload_id: the upload to run the DR pipeline against.
        dataset_name: name for the output dataset.
        sample_names: all sample names included in the analysis.
        control_samples: subset of sample_names used as controls (dose = 0).
        sample_metadata: optional 2D array with header row. When omitted, the tool
            auto-fetches sample metadata from the upload and filters to sample_names —
            avoid passing it if the upload already has metadata (saves token overhead).
        dose_column: column in sample_metadata with dose values (default "dose").
        if_exists: deduplication behaviour (default "skip"):
            "skip" — if a DOSE_RESPONSE dataset with dataset_name already exists,
              return its ID without submitting a new job. Safe for crash recovery.
            "run" — always submit a new job.
        log_intensities: log-transform intensities before fitting (default True).
        use_imputed_intensities: use imputed values from a prior NI step (default True).
        normalise: within-sample normalisation — "none" (default), "sum", "median".
        span_rollmean_k: rolling mean window size, 1 = disabled (default 1).
        prop_required_in_protein: min fraction of non-missing values per protein (default 0.5).
    """
    if if_exists == "skip":
        existing_id, err = _find_existing_dr_dataset(upload_id, dataset_name)
        if err:
            return f"Error checking existing jobs: {err}"
        if existing_id:
            return (
                f"Job already exists (skipped). Dataset ID: {existing_id}\n"
                f"Call wait_for_dataset(upload_id='{upload_id}', dataset_id='{existing_id}') "
                f"to check its status."
            )

    ds = get_client().datasets.find_initial_dataset(upload_id)
    if not ds:
        return f"Error: no initial INTENSITY dataset found for upload {upload_id}"

    resolved_metadata = sample_metadata
    if resolved_metadata is None:
        raw = _fetch_upload_sample_metadata(upload_id)
        if raw:
            resolved_metadata = _filter_sample_metadata(raw, sample_names)

    return run_dose_response(
        input_dataset_ids=[str(ds.id)],
        dataset_name=dataset_name,
        sample_names=sample_names,
        control_samples=control_samples,
        sample_metadata=resolved_metadata,
        dose_column=dose_column,
        log_intensities=log_intensities,
        use_imputed_intensities=use_imputed_intensities,
        normalise=normalise,
        span_rollmean_k=span_rollmean_k,
        prop_required_in_protein=prop_required_in_protein,
    )


def _submit_dr_job(
    i: int,
    job: Dict[str, Any],
    existing_cache: Dict[str, Dict[str, str]],
    initial_ds_cache: Dict[str, Optional[str]],
    upload_meta_cache: Dict[str, Optional[List[List[str]]]],
) -> Dict[str, Any]:
    """Process one dose-response job for run_dose_response_bulk."""
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

    job_sample_names = job["sample_names"]
    job_metadata = job.get("sample_metadata")
    if job_metadata is None:
        raw = upload_meta_cache.get(upload_id)
        if raw:
            job_metadata = _filter_sample_metadata(raw, job_sample_names)

    try:
        run_result = run_dose_response(
            input_dataset_ids=[initial_id],
            dataset_name=dataset_name,
            sample_names=job_sample_names,
            control_samples=job["control_samples"],
            sample_metadata=job_metadata,
            dose_column=job.get("dose_column", "dose"),
            log_intensities=job.get("log_intensities", True),
            use_imputed_intensities=job.get("use_imputed_intensities", True),
            normalise=job.get("normalise", "none"),
            span_rollmean_k=job.get("span_rollmean_k", 1),
            prop_required_in_protein=job.get("prop_required_in_protein", 0.5),
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
def run_dose_response_bulk(jobs: List[Dict[str, Any]]) -> str:
    """Submit multiple dose-response jobs in a single call (max 500 per call).

    Resolves input_dataset_ids from upload_id automatically, with per-upload
    prefetching so list_by_upload is called at most once per unique upload.
    Jobs are submitted in parallel (up to 20 concurrent connections).
    All jobs are attempted regardless of individual failures — errors are
    captured inline and never abort the remaining jobs.

    Default if_exists="skip" makes this safe to re-run after a crash: jobs that
    already completed are returned immediately with their existing dataset ID.

    Each job spec (dict):
      upload_id         str   — upload to run against (required)
      dataset_name      str   — name for the output dataset (required)
      sample_names      list  — all sample names (required)
      control_samples   list  — control sample names (required)
      sample_metadata   list  — 2D array with header row; if omitted, the tool
                                auto-fetches from the upload and filters to
                                sample_names (recommended — avoids re-transmitting
                                data the server already has)
      dose_column       str   — default "dose"
      log_intensities   bool  — default True
      use_imputed_intensities bool — default True
      normalise         str   — default "none"
      span_rollmean_k   int   — default 1
      prop_required_in_protein float — default 0.5
      if_exists         str   — "skip" (default) or "run"

    Returns JSON array:
      [{index, upload_id, dataset_name, dataset_id?, skipped?, error?, error_code?}]
    Or a JSON error object if len(jobs) > 500.
    """
    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}
    initial_ds_cache: Dict[str, Optional[str]] = {}
    upload_meta_cache: Dict[str, Optional[List[List[str]]]] = {}

    for uid in unique_ids:
        intensity_id, existing = _bulk_prefetch_upload_data(uid, "DOSE_RESPONSE")
        existing_cache[uid] = existing
        initial_ds_cache[uid] = intensity_id
        upload_meta_cache[uid] = _fetch_upload_sample_metadata(uid)

    process_fn = partial(
        _submit_dr_job,
        existing_cache=existing_cache,
        initial_ds_cache=initial_ds_cache,
        upload_meta_cache=upload_meta_cache,
    )
    return _run_jobs_parallel(jobs, process_fn)
