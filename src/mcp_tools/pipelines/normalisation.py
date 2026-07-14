"""Normalisation + imputation + filtration pipeline tools."""

from functools import partial
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from md_python.models.dataset_builders import NormalisationImputationDataset

from .. import mcp
from .._client import get_client
from ._bulk import _MAX_BULK_JOBS, _bulk_prefetch_upload_data, _run_jobs_parallel
from ._errors import format_validation_error

# Per-method defaults applied client-side so the server always receives complete
# params. User-supplied extras override these. The v2 dataset-service schema is
# flat — everything gets merged into a single job_run_params dict.
_NORM_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "median": {
        "include_imputed_values": False,
        "median_normalisation_centre_at_zero": True,
    },
    "quantile": {"include_imputed_values": False},
    "sum": {"include_imputed_values": False},
    "batch correction": {"include_imputed_values": False},
    "cpm": {"prior_count": 0},
}
_IMP_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "mnar": {"std_position": 1.8, "std_width": 0.3},
    "knn": {"n_neighbors": 3, "weights": "uniform"},
    "knn_tn": {"knn_tn_k": 5, "knn_tn_distance": "truncation"},
    "set to constant": {"constant_value": 0},
    "mindet": {"q": 0.01},
}
_FILT_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "by missing values": {
        "filter_valid_values_criteria": "percentage",
        "filter_threshold_proportion": 0.5,
        "filter_valid_values_logic": "at least one condition",
    },
    "by minimum abundance": {
        "minimum_abundance_threshold": 0,
        "filter_valid_values_criteria": "percentage",
        "filter_threshold_proportion": 0.5,
        "filter_valid_values_logic": "at least one condition",
    },
    "by ptm localization probability": {"threshold": 0.5},
}

# Keys that the builder accepts as typed kwargs. We forward these from the
# merged extras dict to the builder constructor so that validation sees them.
# Everything else stays in extra_params as a forward-compat escape hatch.
_BUILDER_TYPED_KEYS = (
    # normalisation
    "include_imputed_values",
    "median_normalisation_centre_at_zero",
    "prior_count",
    "batch_correction_technique",
    "batch_variables",
    "batch_variable_combat",
    "reference_batch_combat",
    "mean_only",
    "design_variables",
    "experiment_design",
    # imputation
    "std_position",
    "std_width",
    "n_neighbors",
    "weights",
    "knn_tn_k",
    "knn_tn_distance",
    "constant_value",
    "q",
    # filtration
    "threshold",
    "minimum_abundance_threshold",
    "filter_valid_values_criteria",
    "filter_threshold_proportion",
    "filter_threshold_count",
    "filter_valid_values_logic",
    "filter_based_on_condition",
)


def _split_typed_kwargs(merged: Dict[str, Any]) -> Dict[str, Any]:
    """Pop builder-known kwargs out of *merged* and return them as a kwargs dict.

    Mutates *merged* — anything left behind stays in extra_params.
    """
    typed: Dict[str, Any] = {}
    for key in _BUILDER_TYPED_KEYS:
        if key in merged:
            typed[key] = merged.pop(key)
    return typed


# Re-exported for backward compatibility; the implementation now lives in
# _errors.py so run_gsea (and future tools) share one error envelope.
_format_validation_error = format_validation_error


@mcp.tool()
def run_normalisation_imputation(
    input_dataset_ids: List[str],
    dataset_name: str,
    normalisation_method: str,
    imputation_method: str,
    entity_type: str = "protein",
    filtration_method: Optional[str] = None,
    normalisation_extra_params: Optional[Dict[str, Any]] = None,
    imputation_extra_params: Optional[Dict[str, Any]] = None,
    filtration_extra_params: Optional[Dict[str, Any]] = None,
) -> str:
    """Run normalisation + imputation (with optional pre-filtration) on an INTENSITY dataset.

    Returns: prose. Exact string "Normalisation/imputation pipeline started.
    Dataset ID: <uuid>" on success. The "Dataset ID:" sentinel is stable
    and is parsed by run_normalisation_imputation_bulk.

    The output dataset has type INTENSITY (not NORMALISATION_IMPUTATION); this
    is correct, do not flag it. Once an upload has more than one INTENSITY
    dataset, find_initial_dataset disambiguates by selecting the unique one
    with no upstream input — NI/filter outputs always carry an upstream input.

    Use this when: the user has a COMPLETED upload and wants normalised,
    imputed, and/or filtered intensity data for downstream pairwise / anova /
    dose-response. Do NOT use this when processing many uploads at once — use
    run_normalisation_imputation_bulk.

    ── FILTER-ONLY PATTERN ────────────────────────────────────────────────────
    Pass normalisation_method="skip" + imputation_method="skip" + a filtration
    method to produce a filtered INTENSITY dataset without changing values.
    Common when a downstream pipeline needs only complete entities.

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this table to the user and wait for explicit confirmation. Do NOT
    silently pick defaults — even for simple runs.

    Parameter             Default       Allowed values
    ─────────────────────────────────────────────────────────────────────────────
    normalisation_method  (required)    "median" (recommended for most DDA),
                                        "quantile", "sum", "batch correction",
                                        "cpm" (gene only), "skip"
    imputation_method     (required)    "mnar" (recommended for standard DDA),
                                        "knn", "knn_tn", "global_median",
                                        "median_by_entity", "mindet",
                                        "set to constant", "set to missing", "skip"
    entity_type           "protein"     "protein" | "peptide" | "gene" |
                                        "metabolite" | "ptm" (lowercase on
                                        the wire; metabolite NI is currently
                                        upstream-gated by md-converter and
                                        will likely 422 — see metabolite
                                        pipeline gap memory)
    filtration_method     None ("skip") see filtration table below

    Method-specific params (defaults sent automatically; confirm with user):
      mnar      std_position 1.8, std_width 0.3
      knn       n_neighbors 3, weights "uniform"
      knn_tn    knn_tn_k 5, knn_tn_distance "truncation"
      mindet    q 0.01
      set to constant   constant_value 0 (ask user)
      median    median_normalisation_centre_at_zero True, include_imputed_values False
      quantile/sum/batch correction   include_imputed_values False
      cpm       prior_count 0 (gene only)

    If normalisation_method == "batch correction", you MUST also choose:
      batch_correction_technique   (REQUIRED)  protein/peptide:
                                                 "limma remove batch effect" | "combat"
                                                gene also allows: "combat seq"
      Then technique-specific keys via normalisation_extra_params:
        limma remove batch effect:
          batch_variables   list[{column,type:"categorical"}]  REQUIRED
          design_variables  list[{column,type}]                optional
        combat:
          batch_variable_combat   single column name           REQUIRED
          design_variables        list                         optional
          mean_only               bool, default False          optional
          reference_batch_combat  value from batch column      optional
        combat seq (gene only):
          batch_variable_combat                                REQUIRED
          design_variables                                     optional
        experiment_design         REQUIRED for any technique
                                  — see EXPERIMENT_DESIGN SHAPES below

    Decision rules for batch correction:
      • combat: empirical-Bayes correction; use when batches are confounded but
        each batch has ≥3 samples. Single batch column.
      • limma remove batch effect: linear-model adjustment; use when batches
        are well-separated. Supports multiple batch columns.
      • combat seq: count-data variant; gene/RNA-seq only.

    ── FILTRATION TABLE (entity-keyed) ───────────────────────────────────────
    entity_type   filtration_method           method-specific params
    ─────────────────────────────────────────────────────────────────────────────
    protein       skip                         —
    protein       by missing values            shared filter block (see below)
    peptide       skip                         —
    peptide       by missing values            shared filter block
    peptide       by ptm localization probability   threshold (0–1, default 0.5)
    gene          skip                         —
    gene          by minimum abundance         minimum_abundance_threshold (0–100)
                                                + shared filter block
    ptm           skip                         —
    ptm           by missing values            shared filter block
    ptm           by ptm localization probability   threshold (0–1, default 0.5)
    metabolite    skip                         —
    metabolite    by missing values            shared filter block (upstream NI
                                                support for metabolite is
                                                currently absent — likely 422)

    Shared filter block (by missing values, by minimum abundance):
      filter_valid_values_criteria   "percentage" | "count"
      filter_threshold_proportion    0.0–1.0 (when criteria=percentage)
      filter_threshold_count         integer ≥1 (when criteria=count)
      filter_valid_values_logic      "all conditions" | "at least one condition" |
                                     "full experiment"
      filter_based_on_condition      column name (REQUIRED for the first two
                                     logic values)
      experiment_design              REQUIRED — see EXPERIMENT_DESIGN SHAPES below

    Pass these via filtration_extra_params={...}.

    ── CONDITIONAL REQUIREMENTS (get these right on the FIRST call) ──────────
    Some params are required only because of ANOTHER param's value. There is no
    way to express that in this flat signature, so the rules are listed here.
    Violating one returns "Error: ..." (never a raised exception) naming the
    missing param, the trigger, and what to pass.

    Required param              …when this param has this value
    ─────────────────────────────────────────────────────────────────────────────
    batch_correction_technique  normalisation_method = "batch correction"
    batch_variables             batch_correction_technique = "limma remove batch
                                effect"   (list of {column, type:"categorical"})
    batch_variable_combat       batch_correction_technique = "combat" | "combat seq"
                                (single column name, str)
    experiment_design           normalisation_method = "batch correction"
                                OR filtration_method = "by missing values"
                                OR filtration_method = "by minimum abundance"
    filter_valid_values_criteria  filtration_method = "by missing values" |
                                "by minimum abundance"   ("percentage" | "count")
    filter_threshold_proportion  filter_valid_values_criteria = "percentage"
                                (defaulted to 0.5 if omitted)
    filter_threshold_count      filter_valid_values_criteria = "count"
    filter_based_on_condition   filter_valid_values_logic = "all conditions" |
                                "at least one condition"  (a column name from
                                experiment_design, e.g. "condition"). Not needed
                                for "full experiment".

    ── EXPERIMENT_DESIGN SHAPES (both accepted) ──────────────────────────────
    1. Column-oriented dict — SampleMetadata.to_columns():
         {"sample_name": ["s1", "s2"], "condition": ["ctrl", "treated"]}
    2. Row-oriented list of lists — header row + data rows, exactly what
       get_dataset / get_upload_sample_metadata / load_metadata_from_csv return:
         [["sample_name", "condition"], ["s1", "ctrl"], ["s2", "treated"]]
    The row form is coerced to the column form automatically, so an
    experiment_design copied straight out of get_dataset can be passed through
    unchanged. Rows must not be ragged and the header row must be column names.
    Never hand-construct it — sample names must be verbatim.

    ── SCOPE NOTE ────────────────────────────────────────────────────────────
    Pairwise additions (HR, edgeR, DESeq2) are NOT exposed by this MCP. Pairwise
    comparison ships as limma-only. Do not promise otherwise.

    ── PARAMETER NAME CONVENTION ─────────────────────────────────────────────
    Wire-format strings use the converter canonical (spaced) form:
    "batch correction", "by missing values", "by ptm localization probability",
    "by minimum abundance", "limma remove batch effect", "combat seq".
    Underscored aliases ("batch_correction", "minimum_abundance", etc.) are
    accepted on input for backward compatibility but are deprecated — prefer the
    spaced form.

    Errors:
      - "Error: <message>" (prose envelope, NOT an exception): local validation
        failed before submission — a conditional requirement above was not met,
        an enum value is invalid, a numeric knob is out of range, or
        experiment_design was neither of the two accepted shapes. The message
        names the missing/invalid param and what to pass; fix it and re-call.
      - APIError 422: raised by the server — bad entity_type, missing input
        dataset, or an upstream-gated entity_type (metabolite).
    """
    merged: Dict[str, Any] = {}
    merged.update(_NORM_DEFAULTS.get(normalisation_method, {}))
    if normalisation_extra_params:
        merged.update(normalisation_extra_params)
    merged.update(_IMP_DEFAULTS.get(imputation_method, {}))
    if imputation_extra_params:
        merged.update(imputation_extra_params)
    if filtration_method:
        merged.update(_FILT_DEFAULTS.get(filtration_method, {}))
    if filtration_extra_params:
        merged.update(filtration_extra_params)

    typed = _split_typed_kwargs(merged)
    try:
        dataset_id = NormalisationImputationDataset(
            input_dataset_ids=input_dataset_ids,
            dataset_name=dataset_name,
            normalisation_method=normalisation_method,
            imputation_method=imputation_method,
            entity_type=entity_type,
            filtration_method=filtration_method,
            extra_params=merged or None,
            **typed,
        ).run(get_client())
    except ValidationError as e:
        # ValidationError subclasses ValueError — catch it first and flatten.
        return f"Error: {_format_validation_error(e)}"
    except ValueError as e:
        # Local (pre-submission) validation: conditional-required params, bad
        # enum values, out-of-range knobs. Surfaced as the prose error envelope
        # so the LLM gets a recovery path instead of an uncaught exception.
        return f"Error: {e}"
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
            filtration_method=job.get("filtration_method"),
            normalisation_extra_params=job.get("normalisation_extra_params"),
            imputation_extra_params=job.get("imputation_extra_params"),
            filtration_extra_params=job.get("filtration_extra_params"),
        )
        if "Dataset ID:" in run_result:
            entry["dataset_id"] = run_result.split("Dataset ID:")[-1].strip()
        elif run_result.startswith("Error: "):
            # Local validation failure — the single-job tool returns the prose
            # error envelope rather than raising, so map it back onto the bulk
            # envelope or the job would be miscounted as submitted.
            entry["error"] = run_result.removeprefix("Error: ")
            entry["error_code"] = "invalid_params"
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
      normalisation_method     str   — see run_normalisation_imputation (required)
      imputation_method        str   — see run_normalisation_imputation (required)
      entity_type              str   — "protein" (default), "peptide", "gene",
                                       "metabolite", or "ptm" (lowercase on
                                       the wire; metabolite NI is upstream-
                                       gated and may 422)
      filtration_method        str   — optional; "skip" | "by missing values" |
                                       "by ptm localization probability" |
                                       "by minimum abundance" (entity-specific)
      normalisation_extra_params dict — extra kwargs for normalisation (optional).
                                        For batch correction: include
                                        batch_correction_technique, batch_variables
                                        OR batch_variable_combat, design_variables,
                                        experiment_design.
      imputation_extra_params    dict — extra kwargs for imputation (optional)
      filtration_extra_params    dict — extra kwargs for filtration (optional).
                                        For "by missing values" / "by minimum
                                        abundance": filter_valid_values_criteria,
                                        filter_threshold_proportion or
                                        filter_threshold_count,
                                        filter_valid_values_logic,
                                        filter_based_on_condition, experiment_design.
      if_exists                str   — "skip" (default) or "run"

    Returns JSON envelope:
      {
        "summary": {"total": N, "submitted": int, "skipped": int,
                    "failed": int, "failed_indices": [int, ...]},
        "results": [{index, upload_id, dataset_name, dataset_id?, skipped?,
                     error?, error_code?}, ...],
      }
    The summary sits ABOVE the results so a partial failure can't be missed —
    if summary.failed > 0, walk results at summary.failed_indices to see why.
    Or a JSON error object (no `results` key) if len(jobs) > 500.
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
