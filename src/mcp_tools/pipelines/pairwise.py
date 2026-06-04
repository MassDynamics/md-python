"""Pairwise comparison pipeline tools."""

import json
from functools import partial
from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import PairwiseComparisonDataset
from md_python.models.dataset_builders._methods import (
    _APEGLM_SEED_RANGE,
    _DE_METHODS_PER_ENTITY,
    _DESEQ2_ALPHA_RANGE,
    _DESEQ2_LFC_SHRINKAGE,
    _EDGER_NORM_METHODS,
)
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

    !! CRITICAL — after calling this tool !!
    Pass the ENTIRE returned list as condition_comparisons to ONE single
    run_pairwise_comparison call. Do NOT loop over the pairs and submit
    one call per pair. All pairs must go into one call so limma models
    all contrasts jointly — this is required for correct FDR correction.
    One dataset_id is returned; it covers every pair in the list.
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
    de_method: str = "limma",
    edger_norm_method: str = "TMM",
    deseq2_lfc_shrinkage: str = "none",
    deseq2_alpha: float = 0.05,
    apeglm_seed: int = 1,
    control_variables: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Run a pairwise differential abundance analysis (limma; or edgeR/DESeq2 for genes).

    Returns: prose. Exact string "Pairwise comparison pipeline started.
    Dataset ID: <uuid>" on success. The "Dataset ID:" sentinel is stable
    and parsed by run_pairwise_comparison_bulk.

    Use this when: the user wants case-vs-control differential testing
    between specific pairs of conditions. All pairs go into ONE call so
    limma computes a joint FDR correction.

    Do NOT use this when: the user wants an omnibus test across 3+
    conditions (use run_anova); when processing many uploads (use
    run_pairwise_comparison_bulk).

    entity_type="gene" supports THREE DE engines via de_method: "limma"
    (default), "edgeR", and "DESeq2" (gate: MDFlexiComparisons process_r.py
    de_method_gene; see the de_method row below). protein/peptide/metabolite/
    ptm are limma-only. The engine determines the input dataset: limma takes
    pre-normalised values (e.g. a CPM dataset), while edgeR/DESeq2 take RAW
    integer counts and normalise + low-count-filter internally.

    Parameter defaults are cited to
    tmp/audit_refs/data-set-service/flows/pairwise_comparison/pairwise_comparison_params.py:
      fit_separate_models=True (:65-69), limma_trend=True (:53-57),
      robust_empirical_bayes=True (:59-63), filter_threshold_percentage=0.5
      (:22-30), filter_valid_values_logic="at least one condition" (:79-85).

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter                    Default                  Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    condition_comparisons        (from generate_*)        ALL pairs as one list.
                                                          Do NOT make one call per
                                                          pair — see note below.
    entity_type                  "protein"                "protein" | "peptide" | "gene" |
                                                          "metabolite" | "ptm"
                                                          (lowercase on the wire — UI
                                                          shows "PTM" / "Metabolite")
    de_method                    "limma"                  "limma" (default; only choice
                                                          for protein/peptide/metabolite/
                                                          ptm).
                                                          GENE only: also "edgeR" or
                                                          "DESeq2". The MCP rejects
                                                          edgeR/DESeq2 for any non-gene
                                                          entity_type before submission.
                                                          Wire field is entity-keyed —
                                                          ``de_method_<entity_type>``.
    edger_norm_method            "TMM"                    Only when de_method='edgeR'.
                                                          One of: TMM | RLE |
                                                          upperquartile | none.
    deseq2_lfc_shrinkage         "none"                   Only when de_method='DESeq2'.
                                                          One of: none | apeglm | ashr |
                                                          normal. "apeglm" is the
                                                          modern default for ranking.
    deseq2_alpha                 0.05                     Only when de_method='DESeq2'.
                                                          Float 0–1. SET TO THE FDR
                                                          THRESHOLD YOU WILL APPLY
                                                          DOWNSTREAM — DESeq2's
                                                          independent filtering loses
                                                          power if mismatched.
    apeglm_seed                  1                        Only when de_method='DESeq2'
                                                          AND deseq2_lfc_shrinkage=
                                                          'apeglm'. RNG seed for
                                                          reproducibility.
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

    entity_type: "protein" (default), "peptide", "gene", "metabolite", or
      "ptm". Must match the entity type in the upstream intensity dataset.
      Wire format is lowercase (the UI shows "PTM" / "Metabolite" but the
      backend stores them lowercase — confirmed against live job_run_params
      2026-05-27). Gene pairwise supports limma (default), edgeR, and DESeq2
      via de_method (see the de_method row above); edgeR/DESeq2 are gene-only
      and take raw integer counts. Metabolite / ptm are limma-only.

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

    control_variables: optional list of covariate dicts to add to the limma
      design matrix. Each entry has the shape:
        {"column": "<column_name>", "type": "categorical" | "numerical"}
      Source-of-truth: MDFlexiComparisons ControlValue model (process_r.py:69-71).
      Examples:
        [{"column": "batch", "type": "categorical"},
         {"column": "age",   "type": "numerical"}]
      The MCP wraps the list as {"control_variables": [...]} on the wire (the
      ControlVariables model) — pass only the inner list to this tool.
      Use when batch or other known covariates should be regressed out.

    Errors:
      - ValueError: condition_comparisons empty or references unknown
        condition values; bad entity_type.
      - APIError 422: input dataset not an NI output, entity_type mismatch,
        fewer than 2 replicates per group.

    Guardrails:
      - ONE call for ALL pairs. Never loop one call per pair — breaks FDR.
      - Gene pairwise uses limma; do not promise edgeR or DESeq2.
    """
    cv: Optional[Dict[str, List[Dict[str, str]]]] = (
        {"control_variables": control_variables} if control_variables else None
    )

    # Surface a guidance warning for gene + fit_separate_models=True. The
    # upstream R flow forces fit_separate_models=False for gene regardless
    # (MDFlexiComparisons/src/md_flexi_comparisons/process_r.py:687). Tell the
    # LLM so it can communicate this back to the user — and STRONGLY recommend
    # they set fit_separate_models=False explicitly.
    warning_prose = ""
    if entity_type == "gene" and fit_separate_models:
        warning_prose = (
            "GENE PAIRWISE WARNING: you passed fit_separate_models=True, but the "
            "upstream R flow forces fit_separate_models=False for entity_type='gene' "
            "(MDFlexiComparisons process_r.py:687). The job will run as a single "
            "joint-model fit regardless. STRONGLY recommended: set "
            "fit_separate_models=False explicitly so the parameter the user sees "
            "matches what the server actually does. Re-confirm with the user.\n\n"
        )

    # DE method gating. Only entity_type='gene' accepts edgeR / DESeq2;
    # protein/peptide/metabolite/ptm are limma-only. Validate here so an invalid
    # combo fails fast with a clear message instead of being rejected downstream
    # by the server. (run_pairwise_comparison_bulk routes through this function,
    # so it inherits the gate.) Allowed values come from _methods.py; this block
    # is intentionally kept separate from anova.py's so the two can diverge.
    allowed_de = _DE_METHODS_PER_ENTITY.get(entity_type)
    if allowed_de is None:
        raise ValueError(
            f"unknown entity_type '{entity_type}'. "
            f"Allowed: {sorted(_DE_METHODS_PER_ENTITY)}"
        )
    if de_method not in allowed_de:
        raise ValueError(
            f"de_method '{de_method}' not allowed for entity_type='{entity_type}'. "
            f"Allowed: {sorted(allowed_de)}"
        )
    if de_method == "edgeR":
        if edger_norm_method not in _EDGER_NORM_METHODS:
            raise ValueError(
                "edger_norm_method must be one of: "
                f"{sorted(_EDGER_NORM_METHODS)} (got '{edger_norm_method}')"
            )
    if de_method == "DESeq2":
        if deseq2_lfc_shrinkage not in _DESEQ2_LFC_SHRINKAGE:
            raise ValueError(
                "deseq2_lfc_shrinkage must be one of: "
                f"{sorted(_DESEQ2_LFC_SHRINKAGE)} (got '{deseq2_lfc_shrinkage}')"
            )
        _alpha_lo, _alpha_hi = _DESEQ2_ALPHA_RANGE
        if not _alpha_lo <= deseq2_alpha <= _alpha_hi:
            raise ValueError(
                f"deseq2_alpha must be between {_alpha_lo} and {_alpha_hi} "
                f"(got {deseq2_alpha})"
            )
        if deseq2_lfc_shrinkage == "apeglm":
            _seed_lo, _seed_hi = _APEGLM_SEED_RANGE
            if not _seed_lo <= apeglm_seed <= _seed_hi:
                raise ValueError(
                    f"apeglm_seed must be between {_seed_lo} and {_seed_hi} "
                    f"(got {apeglm_seed})"
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
        de_method=de_method,
        edger_norm_method=edger_norm_method,
        deseq2_lfc_shrinkage=deseq2_lfc_shrinkage,
        deseq2_alpha=deseq2_alpha,
        apeglm_seed=apeglm_seed,
        control_variables=cv,
    ).run(get_client())
    return (
        f"{warning_prose}Pairwise comparison pipeline started. Dataset ID: {dataset_id}"
    )


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
            de_method=job.get("de_method", "limma"),
            edger_norm_method=job.get("edger_norm_method", "TMM"),
            deseq2_lfc_shrinkage=job.get("deseq2_lfc_shrinkage", "none"),
            deseq2_alpha=job.get("deseq2_alpha", 0.05),
            apeglm_seed=job.get("apeglm_seed", 1),
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
      entity_type             str        — "protein" (default), "peptide", "gene",
                                            "metabolite", or "ptm" (lowercase on the
                                            wire)
      de_method               str        — "limma" (default). Gene only also accepts
                                            "edgeR" / "DESeq2". Rejected client-side
                                            for any non-gene entity_type with a
                                            count-engine value.
      edger_norm_method       str        — "TMM" (default). Only used when
                                            de_method='edgeR'. One of: TMM | RLE |
                                            upperquartile | none.
      deseq2_lfc_shrinkage    str        — "none" (default). Only used when
                                            de_method='DESeq2'. One of: none |
                                            apeglm | ashr | normal.
      deseq2_alpha            float      — 0.05 (default). Only used when
                                            de_method='DESeq2'. Set to the FDR
                                            threshold the user will apply downstream.
      apeglm_seed             int        — 1 (default). Only used when
                                            de_method='DESeq2' AND
                                            deseq2_lfc_shrinkage='apeglm'.
      control_variables       list       — optional covariates
      if_exists               str        — "skip" (default) or "run"

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

    for uid in unique_ids:
        _, existing = _bulk_prefetch_upload_data(uid, "PAIRWISE_COMPARISON")
        existing_cache[uid] = existing

    process_fn = partial(_submit_pc_job, existing_cache=existing_cache)
    return _run_jobs_parallel(jobs, process_fn)
