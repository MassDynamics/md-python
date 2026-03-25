import json
from typing import Any, Dict, List, Optional, Tuple

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
      1. Call describe_pipeline("pairwise_comparison") to confirm valid parameter values.
      2. Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
         NEVER construct sample_metadata manually — sample names must be read verbatim.
      3. Use generate_pairwise_comparisons to build condition_comparisons.

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

    MINIMUM DATA REQUIREMENTS:
      - At least 3 distinct dose levels in sample_metadata[dose_column]
      - At least 5 total replicates across all doses (3+ per dose recommended)
      - control_samples are the samples at dose = 0 (excluded from curve fitting,
        used to anchor the baseline)
    The pipeline will return an error if these minimums are not met.

    BEFORE calling this tool:
      1. Call describe_pipeline("dose_response") to confirm valid parameter values.
      2. Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
         NEVER construct sample_metadata, sample_names, or control_samples manually —
         all sample names must be read verbatim from the file to avoid mismatches.

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

    Resolves the input_dataset_ids automatically by looking up the initial
    INTENSITY dataset for the upload — you do not need to call find_initial_dataset
    first. This is the recommended tool when running a single DR job per upload.

    For submitting many DR jobs at once, use run_dose_response_bulk instead.

    sample_metadata is OPTIONAL. When omitted, the tool fetches the sample
    metadata that was already submitted with the upload and filters it to the
    rows matching sample_names. This eliminates the need to re-read the CSV
    file and re-transmit data the server already owns — pass only sample_names,
    control_samples, and the dose_column name.

    if_exists controls deduplication:
      "skip" (default) — if a DOSE_RESPONSE dataset with the same dataset_name
        already exists for this upload, return its ID without submitting a new job.
        Safe to use when resuming after a crash.
      "run" — always submit a new job, even if one with the same name exists.

    All other parameters are identical to run_dose_response. See
    describe_pipeline("dose_response") for full parameter documentation.
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
    """Submit multiple dose-response jobs in a single call.

    Resolves input_dataset_ids from upload_id automatically for each job, with
    per-upload caching so the initial dataset is looked up at most once per upload.
    All jobs are attempted regardless of individual failures — errors are captured
    inline and never abort the remaining jobs.

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
    """
    c = get_client()
    initial_ds_cache: Dict[str, Optional[str]] = {}  # upload_id -> dataset_id or None
    upload_meta_cache: Dict[str, Optional[List[List[str]]]] = (
        {}
    )  # upload_id -> raw metadata
    results = []

    for i, job in enumerate(jobs):
        upload_id = job.get("upload_id", "")
        dataset_name = job.get("dataset_name", "")
        if_exists = job.get("if_exists", "skip")
        entry: Dict[str, Any] = {
            "index": i,
            "upload_id": upload_id,
            "dataset_name": dataset_name,
        }

        # Deduplication check
        if if_exists == "skip":
            existing_id, err = _find_existing_dr_dataset(upload_id, dataset_name)
            if err:
                entry["error"] = f"Could not check existing jobs: {err}"
                entry["error_code"] = "check_failed"
                results.append(entry)
                continue
            if existing_id:
                entry["dataset_id"] = existing_id
                entry["skipped"] = True
                results.append(entry)
                continue

        # Resolve initial dataset (cached)
        if upload_id not in initial_ds_cache:
            try:
                ds = c.datasets.find_initial_dataset(upload_id)
                initial_ds_cache[upload_id] = str(ds.id) if ds else None
            except Exception as e:
                initial_ds_cache[upload_id] = None
                entry["error"] = f"Could not find initial dataset: {e}"
                entry["error_code"] = "dataset_not_found"
                results.append(entry)
                continue

        initial_id = initial_ds_cache[upload_id]
        if not initial_id:
            entry["error"] = f"No initial dataset found for upload {upload_id}"
            entry["error_code"] = "dataset_not_found"
            results.append(entry)
            continue

        # Resolve sample_metadata: use provided value, or auto-fetch from upload
        job_sample_names = job["sample_names"]
        job_metadata = job.get("sample_metadata")
        if job_metadata is None:
            if upload_id not in upload_meta_cache:
                upload_meta_cache[upload_id] = _fetch_upload_sample_metadata(upload_id)
            raw = upload_meta_cache[upload_id]
            if raw:
                job_metadata = _filter_sample_metadata(raw, job_sample_names)

        # Submit job
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

        results.append(entry)

    return json.dumps(results, indent=2)
