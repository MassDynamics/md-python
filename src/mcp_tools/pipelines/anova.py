"""ANOVA pipeline tool."""

from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import MinimalDataset
from md_python.models.dataset_builders._methods import (
    _DE_METHODS_PER_ENTITY,
    _ENTITY_TYPES,
    _de_method_key,
)
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_anova(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: List[List[str]],
    condition_column: str,
    comparisons_type: str = "all",
    condition_comparisons: Optional[List[List[str]]] = None,
    filter_method: str = "percentage",
    filter_threshold_percentage: float = 0.5,
    filter_threshold_count: Optional[int] = None,
    filter_valid_values_logic: str = "at least one condition",
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
    """Run an ANOVA-based differential abundance analysis across multiple conditions.

    Returns: prose. Exact string "ANOVA pipeline started. Dataset ID: <uuid>"
    on success. Branch on the "Dataset ID:" sentinel.

    Use this when: the user wants an omnibus test across 3+ condition levels
    to detect any difference, on any entity type (protein / peptide / gene).
    Gene-level ANOVA runs through limma (mdFlexiComparisons runANOVA.R,
    de_method="limma"). edgeR / DESeq2 are NOT exposed by this MCP.

    Do NOT use this when: only two conditions are being compared (use
    run_pairwise_comparison — ANOVA reduces to a t-test and pairwise gives
    direction information). ANOVA tells you some groups differ but NOT which
    specific pairs differ — follow up with run_pairwise_comparison for
    targeted contrasts.

    Parameter defaults cited to
    tmp/audit_refs/MDFlexiComparisons/R/limmaStatsFun.R and runANOVA.R
    (limma_trend + robust_empirical_bayes passed directly to eBayes;
    filter_threshold_percentage=0.5 is the limma-standard completeness floor).

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter                    Default                  Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    comparisons_type             "all"                    "all" — test all pairwise
                                                            combinations across all
                                                            condition levels at once.
                                                          "custom" — restrict to
                                                            specific [case, control]
                                                            pairs via condition_comparisons.
    condition_comparisons        None                     Required only when
                                                            comparisons_type="custom".
                                                          List of [case, control] pairs,
                                                            e.g. [["treated","control"]].
    filter_method                "percentage"             "percentage" | "count"
    filter_threshold_percentage  0.5 (50 %)               float 0.0 – 1.0
                                                          (used when filter_method=
                                                           "percentage")
    filter_threshold_count       None                     int >= 1; REQUIRED when
                                                          filter_method="count"
    filter_valid_values_logic    "at least one            "at least one condition" |
                                  condition"               "all conditions" |
                                                           "full experiment"
    limma_trend                  True                     True | False
    robust_empirical_bayes       True                     True | False
    entity_type                  "protein"                "protein" | "peptide" | "gene" |
                                                          "metabolite" | "ptm"
                                                          (lowercase on the wire — UI
                                                          shows "PTM" / "Metabolite").
    de_method                    "limma"                  "limma" (default; only choice
                                                          for protein/peptide/metabolite/
                                                          ptm).
                                                          GENE only: also "edgeR" or
                                                          "DESeq2". Wire field is
                                                          entity-keyed —
                                                          ``de_method_<entity_type>``.
    edger_norm_method            "TMM"                    Only when de_method='edgeR'.
                                                          One of: TMM | RLE |
                                                          upperquartile | none.
    deseq2_lfc_shrinkage         "none"                   Only when de_method='DESeq2'.
                                                          One of: none | apeglm | ashr |
                                                          normal.
    deseq2_alpha                 0.05                     Only when de_method='DESeq2'.
                                                          Float 0–1. Set to the FDR
                                                          threshold you will apply
                                                          downstream.
    apeglm_seed                  1                        Only when de_method='DESeq2'
                                                          AND deseq2_lfc_shrinkage=
                                                          'apeglm'.
    control_variables            None                     list of {column, type:
                                                          "categorical"|"numerical"}
                                                          covariate dicts; e.g.
                                                          [{"column": "batch",
                                                            "type": "categorical"}].

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    BEFORE calling this tool:
      Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
      NEVER construct sample_metadata manually — sample names must be read verbatim.
      Call describe_pipeline("anova") if you need the full parameter schema.

    sample_metadata: pass load_metadata_from_csv["sample_metadata"] directly.
    condition_column: the column defining the groups to compare (e.g. "condition").

    filter_valid_values_logic controls which proteins pass the completeness filter:
      "at least one condition" (default) — keep rows with enough valid values in at
        least one compared condition. Good for most experiments.
      "all conditions" — require completeness in every compared condition.
      "full experiment" — require completeness across the entire experiment.

    filter_method controls the completeness criterion:
      "percentage" (default) — filter_threshold_percentage in [0,1] is the
        fraction of valid (non-missing) samples per condition that an entity
        must reach. Default 0.5.
      "count" — filter_threshold_count is the minimum integer count of valid
        samples per condition. Required when filter_method="count".

    control_variables: optional covariates to include in the limma design
      matrix (batch, sex, age, etc.). Each entry has the shape
      {"column": str, "type": "categorical" | "numerical"}.
      Source-of-truth:
      MDFlexiComparisons/src/md_flexi_comparisons/process_r.py:69-71 (entry
      shape) and process_r.py:380-386 (ANOVAParamsProperties.control_variables).
      The MCP wraps the list as {"control_variables": [...]} on the wire.

    Errors:
      - ValueError: comparisons_type="custom" without condition_comparisons;
        filter_method="count" without filter_threshold_count;
        bad entity_type or control_variables shape.
      - APIError 422: fewer than 3 condition levels, input dataset not an
        NI output.
    """
    sm = SampleMetadata(data=sample_metadata)
    experiment_design: Dict[str, Any] = dict(sm.to_columns())

    if filter_method not in {"percentage", "count"}:
        raise ValueError(
            f"filter_method must be 'percentage' or 'count' (got '{filter_method}')"
        )
    filter_values_criteria: Dict[str, Any] = {"method": filter_method}
    if filter_method == "percentage":
        if not 0.0 <= filter_threshold_percentage <= 1.0:
            raise ValueError(
                "filter_threshold_percentage must be between 0 and 1 "
                f"(got {filter_threshold_percentage})"
            )
        filter_values_criteria["filter_threshold_percentage"] = (
            filter_threshold_percentage
        )
    else:  # count
        if filter_threshold_count is None or filter_threshold_count < 1:
            raise ValueError(
                "filter_threshold_count (int >= 1) is required when "
                "filter_method='count'"
            )
        filter_values_criteria["filter_threshold_count"] = filter_threshold_count

    if entity_type not in _ENTITY_TYPES:
        raise ValueError(
            f"entity_type must be one of: {sorted(_ENTITY_TYPES)} "
            f"(got '{entity_type}')"
        )

    # DE method gating. Only entity_type='gene' accepts edgeR / DESeq2.
    allowed_de = _DE_METHODS_PER_ENTITY[entity_type]
    if de_method not in allowed_de:
        raise ValueError(
            f"de_method '{de_method}' not allowed for entity_type='{entity_type}'. "
            f"Allowed: {sorted(allowed_de)}"
        )
    if de_method == "edgeR":
        if edger_norm_method not in {"TMM", "RLE", "upperquartile", "none"}:
            raise ValueError(
                "edger_norm_method must be one of: TMM, RLE, upperquartile, none "
                f"(got '{edger_norm_method}')"
            )
    if de_method == "DESeq2":
        if deseq2_lfc_shrinkage not in {"none", "apeglm", "ashr", "normal"}:
            raise ValueError(
                "deseq2_lfc_shrinkage must be one of: none, apeglm, ashr, normal "
                f"(got '{deseq2_lfc_shrinkage}')"
            )
        if not 0.0 <= deseq2_alpha <= 1.0:
            raise ValueError(
                "deseq2_alpha must be between 0 and 1 "
                f"(got {deseq2_alpha})"
            )
        if deseq2_lfc_shrinkage == "apeglm":
            if not 0 <= apeglm_seed <= 2147483647:
                raise ValueError(
                    "apeglm_seed must be between 0 and 2147483647 "
                    f"(got {apeglm_seed})"
                )

    if control_variables is not None:
        if not isinstance(control_variables, list):
            raise ValueError("control_variables must be a list of dicts")
        for cv in control_variables:
            if not isinstance(cv, dict) or "column" not in cv or "type" not in cv:
                raise ValueError(
                    "each control_variables entry must be "
                    "{'column': str, 'type': 'categorical'|'numerical'}"
                )
            if cv["type"] not in {"categorical", "numerical"}:
                raise ValueError(
                    "control_variables[].type must be 'categorical' or 'numerical' "
                    f"(got '{cv['type']}')"
                )

    job_run_params: Dict[str, Any] = {
        "experiment_design": experiment_design,
        "condition_column": condition_column,
        "comparisons_type": comparisons_type,
        "filter_values_criteria": filter_values_criteria,
        "filter_valid_values_logic": filter_valid_values_logic,
        "limma_trend": limma_trend,
        "robust_empirical_bayes": robust_empirical_bayes,
        "entity_type": entity_type,
        # DE method on the wire is entity-keyed: emit ``de_method_<entity>``
        # so the MDFlexiComparisons Pydantic schema picks it up.
        _de_method_key(entity_type): de_method,
    }
    # edgeR / DESeq2 companion params — only emit when the chosen DE method
    # needs them.
    if de_method == "edgeR":
        job_run_params["edger_norm_method"] = edger_norm_method
    elif de_method == "DESeq2":
        job_run_params["deseq2_lfc_shrinkage"] = deseq2_lfc_shrinkage
        job_run_params["deseq2_alpha"] = deseq2_alpha
        if deseq2_lfc_shrinkage == "apeglm":
            job_run_params["apeglm_seed"] = apeglm_seed
    if condition_comparisons:
        # Source-of-truth: MDFlexiComparisons/src/md_flexi_comparisons/process_r.py:91-92
        # ConditionComparisons = {"condition_comparison_pairs": List[Tuple[str,str]]}.
        # The ANOVA params model (ANOVAParamsProperties.condition_comparisons) expects
        # this wrapper, not a bare list.
        job_run_params["condition_comparisons"] = {
            "condition_comparison_pairs": condition_comparisons,
        }
    if control_variables is not None:
        # Source-of-truth:
        # MDFlexiComparisons/src/md_flexi_comparisons/process_r.py:380-386
        # ANOVAParamsProperties.control_variables: Optional[ControlVariables]
        # ControlVariables = {"control_variables": List[ControlVariable]}.
        job_run_params["control_variables"] = {"control_variables": control_variables}

    dataset_id = MinimalDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        job_slug="anova",
        job_run_params=job_run_params,
    ).run(get_client())
    return f"ANOVA pipeline started. Dataset ID: {dataset_id}"
