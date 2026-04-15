"""ANOVA pipeline tool."""

from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import MinimalDataset
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
    filter_valid_values_logic: str = "at least one condition",
    limma_trend: bool = True,
    robust_empirical_bayes: bool = True,
) -> str:
    """Run an ANOVA-based differential abundance analysis across multiple conditions.

    Returns: prose. Exact string "ANOVA pipeline started. Dataset ID: <uuid>"
    on success. Branch on the "Dataset ID:" sentinel.

    Use this when: the user wants an omnibus test across 3+ condition levels
    to detect any difference, or gene-level differential analysis (pairwise
    does not support gene entity_type).

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
    filter_valid_values_logic    "at least one            "at least one condition" |
                                  condition"               "all conditions" |
                                                           "full experiment"
    filter_threshold_percentage  0.5 (50 %)               float 0.0 – 1.0
    limma_trend                  True                     True | False
    robust_empirical_bayes       True                     True | False

    NOTE: ANOVA does not expose entity_type — it always operates at protein level.
    Use run_pairwise_comparison if you need peptide- or gene-level analysis.

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

    filter_threshold_percentage: fraction of samples in a condition that must have
      valid (non-missing) values to pass the filter. Default 0.5 = 50%.
      Only used when filter_method="percentage" (default). "count" mode is
      accepted but the MCP does not currently expose a filter_threshold_count
      knob — callers needing count-mode must drop to the REST client directly.

    Errors:
      - ValueError: comparisons_type="custom" without condition_comparisons.
      - APIError 422: fewer than 3 condition levels, input dataset not an
        NI output.
    """
    sm = SampleMetadata(data=sample_metadata)
    experiment_design: Dict[str, Any] = dict(sm.to_columns())

    filter_values_criteria: Dict[str, Any] = {"method": filter_method}
    if filter_method == "percentage":
        filter_values_criteria["filter_threshold_percentage"] = (
            filter_threshold_percentage
        )

    job_run_params: Dict[str, Any] = {
        "experiment_design": experiment_design,
        "condition_column": condition_column,
        "comparisons_type": comparisons_type,
        "filter_values_criteria": filter_values_criteria,
        "filter_valid_values_logic": filter_valid_values_logic,
        "limma_trend": limma_trend,
        "robust_empirical_bayes": robust_empirical_bayes,
    }
    if condition_comparisons:
        job_run_params["condition_comparisons"] = condition_comparisons

    dataset_id = MinimalDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        job_slug="anova",
        job_run_params=job_run_params,
    ).run(get_client())
    return f"ANOVA pipeline started. Dataset ID: {dataset_id}"
