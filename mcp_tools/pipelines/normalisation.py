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
    """Run a normalisation + imputation pipeline on an INTENSITY dataset.

    Returns: prose. Exact string "Normalisation/imputation pipeline started.
    Dataset ID: <uuid>" on success. The "Dataset ID:" sentinel is stable
    and is parsed by run_normalisation_imputation_bulk.

    Use this when: the user has a COMPLETED upload and wants normalised,
    imputed data for downstream pairwise / anova / dose-response.

    Do NOT use this when: processing many uploads at once — use
    run_normalisation_imputation_bulk. The output dataset has type
    INTENSITY (not NORMALISATION_IMPUTATION); this is correct, do not
    flag it.

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this table to the user and wait for explicit confirmation.
    Do NOT silently pick defaults — even for simple runs.

    Parameter               Default        Options
    ─────────────────────────────────────────────────────────────────────────────
    normalisation_method    (required)     "median" (recommended for most DDA),
                                           "quantile", "global_median",
                                           "batch_correction", "cpm" (gene data),
                                           "skip"
    imputation_method       (required)     "mnar" (recommended for standard DDA),
                                           "knn", "set to constant",
                                           "set to missing", "skip"
    entity_type             "protein"      "protein" | "peptide" | "gene"

    If imputation_method = "mnar" — defaults sent automatically, confirm with user:
      std_position   1.8   left-shift from sample mean (lower = more extreme imputation)
      std_width      0.3   width as fraction of sample std dev

    If imputation_method = "knn" — defaults sent automatically, confirm with user:
      n_neighbors    3     range 1–10
      weights        null  null (uniform) or "distance"

    If imputation_method = "set to constant":
      constant_value (required) — integer 0–100, ask user what value to substitute

    If normalisation_method = "batch_correction":
      batch_variables   (required) — ask user which metadata column defines batches
      experiment_design (required) — pass load_metadata_from_csv output
      design_variables  (recommended) — columns to preserve (e.g. ["condition"])

    If normalisation_method = "cpm" (gene data only):
      prior_count    0     added before CPM calculation

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    entity_type: "protein" (default), "peptide", or "gene".
      Must match the data type in the upstream intensity dataset.

    ── NORMALISATION METHODS ──────────────────────────────────────────────────
    "median"           No extra params. Subtracts per-sample median (log2 space).
                       Robust. RECOMMENDED for most proteomics experiments.
    "quantile"         No extra params. Forces identical quantile distributions.
                       Stronger assumption — ask user if distributions are comparable.
    "skip"             No extra params. Skips normalisation entirely.
    "batch_correction" Requires normalisation_extra_params:
                         batch_variables  List[str]  columns that define batch
                                          (e.g. ["batch"]). REQUIRED.
                         design_variables List[str]  columns encoding biological
                                          design to preserve (e.g. ["condition"]).
                                          Optional but strongly recommended.
                         experiment_design dict  sample metadata as column dict
                                          (pass load_metadata_from_csv output
                                          converted via SampleMetadata.to_columns()).
                                          REQUIRED for ComBat correction.
    "cpm"              Gene data only. Optional extra param:
                         prior_count  float  default 0. Added before CPM calculation.

    ── IMPUTATION METHODS ─────────────────────────────────────────────────────
    "mnar"             PREFERRED for standard DDA proteomics (MNAR pattern).
                       Defaults sent automatically: std_position=1.8, std_width=0.3.
                       Override via imputation_extra_params if user requests different values.
                         std_position  float  left-shift from mean (default 1.8)
                         std_width     float  width as fraction of std (default 0.3)
    "knn"              K-nearest neighbours. Better for MAR (missing at random) data.
                       Defaults sent automatically: n_neighbors=3, weights=null.
                       Override via imputation_extra_params if user requests different values.
                         n_neighbors  int   number of neighbours (default 3, range 1–10)
                         weights      str   null (default) or "distance"
    "global_median"    No extra params. Replaces all missing with global median.
    "median_by_entity" No extra params. Replaces each missing with that
                       protein/gene's own median intensity.
    "set to constant"  Required imputation_extra_params:
                         constant_value  int  integer value to substitute for every NaN (range 0–100)
    "set to missing"   No extra params. Sets all values to NaN (removes data).
    "skip"             No extra params. Leaves NaN in output (no imputation).

    Call describe_pipeline("normalisation_imputation") only if you need to
    verify a parameter value you are unsure of — it is not a mandatory
    pre-step.

    Numeric defaults (MNAR std_position=1.8, std_width=0.3; KNN n_neighbors=3,
    weights=None; constant_value=0) are set client-side in normalisation.py
    and sent on every call — the R package (lfq_processing/R/impute_lfq.R)
    treats the arguments as required, so the MCP ships the defaults
    explicitly.

    Errors:
      - APIError 422: required per-method params missing (e.g.
        batch_variables without batch_correction), bad entity_type,
        missing input dataset.
      - ValueError: raised by NormalisationImputationDataset on local
        validation.
    """
    # Per-method defaults — always sent so the server receives complete params.
    # User-supplied extra_params override these.
    _norm_defaults: Dict[str, Dict[str, Any]] = {
        "cpm": {"prior_count": 0},
    }
    _imp_defaults: Dict[str, Dict[str, Any]] = {
        "mnar": {"std_position": 1.8, "std_width": 0.3},
        "knn": {"n_neighbors": 3, "weights": None},
        "set to constant": {"constant_value": 0},
    }

    norm: Dict[str, Any] = {"method": normalisation_method}
    norm.update(_norm_defaults.get(normalisation_method, {}))
    if normalisation_extra_params:
        norm.update(normalisation_extra_params)

    imp: Dict[str, Any] = {"method": imputation_method}
    imp.update(_imp_defaults.get(imputation_method, {}))
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
      normalisation_method     str   — "median", "quantile", "skip",
                                       "batch_correction", or "cpm" (required)
      imputation_method        str   — "mnar", "knn", "global_median",
                                       "median_by_entity", "set to constant",
                                       "set to missing", or "skip" (required)
      entity_type              str   — "protein" (default), "peptide", or "gene"
      normalisation_extra_params dict — extra kwargs for normalisation (optional).
                                       For batch_correction: include batch_variables,
                                       design_variables, and experiment_design (the
                                       sample metadata dict from load_metadata_from_csv).
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
