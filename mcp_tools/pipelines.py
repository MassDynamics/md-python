import concurrent.futures
import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from md_python.models.dataset_builders import (
    DoseResponseDataset,
    NormalisationImputationDataset,
    PairwiseComparisonDataset,
)
from md_python.models.metadata import SampleMetadata

from . import mcp
from ._client import get_client

# ---------------------------------------------------------------------------
# Bulk submission constants — shared by all *_bulk tools.
# ---------------------------------------------------------------------------
_MAX_BULK_JOBS = 500
_BULK_WORKERS = 20
_TERMINAL_STATES: Set[str] = {"COMPLETED", "FAILED", "ERROR", "CANCELLED"}


def _bulk_prefetch_upload_data(
    upload_id: str, existing_type: str
) -> Tuple[Optional[str], Dict[str, str]]:
    """Fetch all datasets for an upload in one API call.

    Returns:
        (intensity_dataset_id_or_none, {dataset_name: dataset_id})
        where the dict contains existing datasets of existing_type only.
    """
    try:
        datasets = get_client().datasets.list_by_upload(upload_id)
        intensity_id = next(
            (str(d.id) for d in datasets if getattr(d, "type", None) == "INTENSITY"),
            None,
        )
        existing = {
            d.name: str(d.id)
            for d in datasets
            if getattr(d, "type", None) == existing_type
        }
        return intensity_id, existing
    except Exception:
        return None, {}


def _run_jobs_parallel(
    jobs: List[Dict[str, Any]],
    process_fn: Callable[[int, Dict[str, Any]], Dict[str, Any]],
) -> str:
    """Validate job count, submit all jobs in parallel, return ordered JSON array.

    Returns an error JSON object (not array) if the job count exceeds _MAX_BULK_JOBS.
    process_fn(index, job) must return a result dict with at least {"index": index}.
    """
    if len(jobs) > _MAX_BULK_JOBS:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. Maximum per call is {_MAX_BULK_JOBS}. "
                    "Split the call into smaller batches."
                )
            }
        )

    def _call(args: Tuple[int, Dict[str, Any]]) -> Dict[str, Any]:
        i, job = args
        return process_fn(i, job)

    with concurrent.futures.ThreadPoolExecutor(max_workers=_BULK_WORKERS) as executor:
        results = list(executor.map(_call, enumerate(jobs)))

    return json.dumps(results, indent=2)


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
        "description": (
            "Fit dose-response curves to intensity data using a four-parameter "
            "log-logistic (4PL) model. "
            "MINIMUM DATA REQUIREMENTS: at least 3 distinct dose levels and at "
            "least 5 total replicates across all doses (3+ replicates per dose "
            "recommended). The pipeline will fail if these minimums are not met."
        ),
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
                "valid_values": ["none", "sum", "median"],
                "description": (
                    "Normalisation to apply before fitting. "
                    "'none' is the standard choice (recommended when data has already been "
                    "normalised upstream, e.g. via run_normalisation_imputation). "
                    "'sum' and 'median' apply within-sample normalisation at the dose-response stage."
                ),
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
    """Return the full parameter schema for a pipeline, including valid_values and defaults.

    Call this when you need to verify valid parameter values before running a pipeline.
    Not required if the parameter values are already known from context or prior calls.

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

    BEFORE calling this tool:
      1. Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
         NEVER construct sample_metadata manually — sample names must be read verbatim.
      2. Use generate_pairwise_comparisons to build condition_comparisons.
      Call describe_pipeline("pairwise_comparison") if you need the full parameter schema.

    sample_metadata: pass load_metadata_from_csv["sample_metadata"] directly.

    entity_type: "protein" (default) for protein-level analysis, "peptide" for peptide-level.
      Use "protein" unless the user explicitly requests peptide-level results.

    filter_valid_values_logic controls which rows (proteins/peptides) pass the
      completeness filter before modelling:
        "at least one condition" (default) — keep rows with enough valid values
          in at least one of the compared conditions. Good for most experiments.
        "all conditions" — require completeness in every condition being compared.
          More stringent; reduces false positives but loses more data.
        "full experiment" — require completeness across the entire experiment.
          Most stringent; use for very clean, complete datasets.

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
    """Run a dose-response curve fitting pipeline (4-parameter log-logistic model).

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


def _filter_sample_metadata(
    metadata: List[List[str]], sample_names: List[str]
) -> List[List[str]]:
    """Return header row + data rows whose sample_name is in sample_names."""
    if not metadata:
        return metadata
    header = metadata[0]
    try:
        sn_idx = [h.strip().lower() for h in header].index("sample_name")
    except ValueError:
        return metadata  # can't filter without sample_name column; return as-is
    sample_set = set(sample_names)
    return [header] + [
        row for row in metadata[1:] if len(row) > sn_idx and row[sn_idx] in sample_set
    ]


def _fetch_upload_sample_metadata(
    upload_id: str,
) -> Optional[List[List[str]]]:
    """Fetch sample_metadata from the upload record, or return None on failure."""
    try:
        upload = get_client().uploads.get_by_id(upload_id)
        if upload and upload.sample_metadata:
            return upload.sample_metadata.data
    except Exception:
        pass
    return None


def _find_existing_dr_dataset(
    upload_id: str, dataset_name: str
) -> Tuple[Optional[str], Optional[str]]:
    """Return (dataset_id, None) if a DOSE_RESPONSE dataset with dataset_name exists,
    or (None, error_string) if the lookup fails, or (None, None) if not found."""
    try:
        datasets = get_client().datasets.list_by_upload(upload_id)
        for ds in datasets:
            if ds.type == "DOSE_RESPONSE" and ds.name == dataset_name:
                return str(ds.id), None
        return None, None
    except Exception as e:
        return None, str(e)


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
    # Deduplication check
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

    # Resolve input dataset
    ds = get_client().datasets.find_initial_dataset(upload_id)
    if not ds:
        return f"Error: no initial INTENSITY dataset found for upload {upload_id}"

    # Auto-fetch sample_metadata from upload if not provided
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
    if len(jobs) > _MAX_BULK_JOBS:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. Maximum per call is {_MAX_BULK_JOBS}. "
                    "Split the call into smaller batches."
                )
            }
        )

    # --- Prefetch per-upload data (one list_by_upload call per unique upload) ---
    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}  # uid -> {name -> dataset_id}
    initial_ds_cache: Dict[str, Optional[str]] = {}  # uid -> INTENSITY dataset_id
    upload_meta_cache: Dict[str, Optional[List[List[str]]]] = {}  # uid -> metadata

    for uid in unique_ids:
        intensity_id, existing = _bulk_prefetch_upload_data(uid, "DOSE_RESPONSE")
        existing_cache[uid] = existing
        initial_ds_cache[uid] = intensity_id
        upload_meta_cache[uid] = _fetch_upload_sample_metadata(uid)

    # --- Submit jobs in parallel ---
    def _process(i: int, job: Dict[str, Any]) -> Dict[str, Any]:
        upload_id = job.get("upload_id", "")
        dataset_name = job.get("dataset_name", "")
        if_exists = job.get("if_exists", "skip")
        entry: Dict[str, Any] = {
            "index": i,
            "upload_id": upload_id,
            "dataset_name": dataset_name,
        }

        if if_exists == "skip":
            existing_id = existing_cache.get(upload_id, {}).get(dataset_name)
            if existing_id:
                entry["dataset_id"] = existing_id
                entry["skipped"] = True
                return entry

        initial_id = initial_ds_cache.get(upload_id)
        if not initial_id:
            entry["error"] = (
                f"No initial INTENSITY dataset found for upload {upload_id}"
            )
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

    return _run_jobs_parallel(jobs, _process)


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
    if len(jobs) > _MAX_BULK_JOBS:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. Maximum per call is {_MAX_BULK_JOBS}. "
                    "Split the call into smaller batches."
                )
            }
        )

    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}
    initial_ds_cache: Dict[str, Optional[str]] = {}

    for uid in unique_ids:
        intensity_id, existing = _bulk_prefetch_upload_data(
            uid, "NORMALISATION_IMPUTATION"
        )
        existing_cache[uid] = existing
        initial_ds_cache[uid] = intensity_id

    def _process(i: int, job: Dict[str, Any]) -> Dict[str, Any]:
        upload_id = job.get("upload_id", "")
        dataset_name = job.get("dataset_name", "")
        if_exists = job.get("if_exists", "skip")
        entry: Dict[str, Any] = {
            "index": i,
            "upload_id": upload_id,
            "dataset_name": dataset_name,
        }

        if if_exists == "skip":
            existing_id = existing_cache.get(upload_id, {}).get(dataset_name)
            if existing_id:
                entry["dataset_id"] = existing_id
                entry["skipped"] = True
                return entry

        initial_id = initial_ds_cache.get(upload_id)
        if not initial_id:
            entry["error"] = (
                f"No initial INTENSITY dataset found for upload {upload_id}"
            )
            entry["error_code"] = "dataset_not_found"
            return entry

        norm: Dict[str, Any] = {"method": job.get("normalisation_method", "")}
        extra_norm = job.get("normalisation_extra_params")
        if extra_norm:
            norm.update(extra_norm)

        imp: Dict[str, Any] = {"method": job.get("imputation_method", "")}
        extra_imp = job.get("imputation_extra_params")
        if extra_imp:
            imp.update(extra_imp)

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

    return _run_jobs_parallel(jobs, _process)


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
    if len(jobs) > _MAX_BULK_JOBS:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. Maximum per call is {_MAX_BULK_JOBS}. "
                    "Split the call into smaller batches."
                )
            }
        )

    unique_ids = list({job.get("upload_id", "") for job in jobs})
    existing_cache: Dict[str, Dict[str, str]] = {}

    for uid in unique_ids:
        _, existing = _bulk_prefetch_upload_data(uid, "PAIRWISE_COMPARISON")
        existing_cache[uid] = existing

    def _process(i: int, job: Dict[str, Any]) -> Dict[str, Any]:
        upload_id = job.get("upload_id", "")
        dataset_name = job.get("dataset_name", "")
        if_exists = job.get("if_exists", "skip")
        entry: Dict[str, Any] = {
            "index": i,
            "upload_id": upload_id,
            "dataset_name": dataset_name,
        }

        if if_exists == "skip":
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

        cv = job.get("control_variables")
        cv_param = {"control_variables": cv} if cv else None

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
                control_variables=cv_param,
            )
            if "Dataset ID:" in run_result:
                entry["dataset_id"] = run_result.split("Dataset ID:")[-1].strip()
            else:
                entry["result"] = run_result
        except Exception as e:
            entry["error"] = str(e)
            entry["error_code"] = "run_failed"

        return entry

    return _run_jobs_parallel(jobs, _process)
