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

    Use when comparing 3 or more groups simultaneously. ANOVA tests for any
    difference across all groups at once; use run_pairwise_comparison when you
    need specific group-vs-group contrasts.

    ALWAYS ask the user which parameters to use before calling this tool, unless
    the user has explicitly asked you to suggest the best option based on their data.

    BEFORE calling this tool:
      Use load_metadata_from_csv to read sample_metadata from the user's CSV file.
      NEVER construct sample_metadata manually — sample names must be read verbatim.
      Call describe_pipeline("anova") if you need the full parameter schema.

    sample_metadata: pass load_metadata_from_csv["sample_metadata"] directly.
    condition_column: the column defining the groups to compare (e.g. "condition").
    comparisons_type: "all" (default) tests all pairwise combinations.
      "custom" restricts to the pairs supplied in condition_comparisons.
    condition_comparisons: required when comparisons_type="custom". List of
      [case, control] pairs, e.g. [["treated", "control"]].

    filter_method / filter_threshold_percentage: control which rows pass the
      valid-value completeness filter before modelling. Default keeps rows with
      at least 50% valid values.
    filter_valid_values_logic: how the filter is applied — "at least one condition"
      (default), "all conditions", or "full experiment".

    Returns the new dataset ID on success.
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
