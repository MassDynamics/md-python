"""CAMERA GSEA (gene-set enrichment) pipeline tool."""

from typing import Any, Dict, List, Optional

from md_python.models.dataset_builders import GseaDataset
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_gsea(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: List[List[str]],
    condition_column: str,
    condition_comparisons: List[List[str]],
    species: str,
    entity_type: str = "protein",
    sets: Optional[List[str]] = None,
    filter_method: str = "percentage",
    filter_threshold_percentage: float = 0.5,
    filter_threshold_count: Optional[int] = None,
    filter_valid_values_logic: str = "at least one condition",
    limma_trend: bool = True,
    robust_empirical_bayes: bool = True,
    fit_separate_models: bool = True,
    control_variables: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Run a CAMERA gene-set enrichment analysis (GSEA).

    CAMERA (Wu & Smyth, 2012) is a competitive gene-set test that accounts
    for inter-gene correlation. It tests whether genes in a set are
    differentially expressed relative to genes outside the set, for each
    pairwise comparison derived from condition_column + condition_comparisons.
    Output is an ENRICHMENT dataset with enrichment p-values, BH-adjusted
    p-values and average fold change per gene set per comparison.

    Returns: prose. Exact string "GSEA pipeline started. Dataset ID: <uuid>"
    on success. The "Dataset ID:" sentinel is stable.

    Use this when: the user wants a threshold-free, whole-ranking enrichment
    test that uses the full differential signal across a comparison. For a
    test over a pre-selected list of hits use run_ora instead.

    Do NOT use this when: there are no condition comparisons to derive the
    differential ranking from — GSEA needs condition_comparisons.

    INPUT REQUIREMENTS:
      * input_dataset_ids: exactly ONE INTENSITY dataset UUID (a DATASET id).
      * sample_metadata: read via load_metadata_from_csv — NEVER construct it
        manually. Must include sample_name and condition_column.

    Backend job slug: "camera_gsea" (output_dataset_type "ENRICHMENT").
    Parameter defaults / enums are from the live job catalogue (/jobs -> slug
    "camera_gsea", EnrichmentParamsProperties).

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter                    Platform default              Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    species                      (required, no default)        "Human" | "Mouse" |
                                                                "Chinese hamster" |
                                                                "Yeast".
    entity_type                  "protein"                      "protein" | "gene".
    sets                         ["GO - Biological Process",    Knowledge bases.
                                  "GO - Cellular Component",     Options depend on
                                  "GO - Molecular Function"]     species.
    condition_comparisons        (required)                     List of
                                                                [case, control]
                                                                pairs.
    filter_method                "percentage"                   "percentage" |
                                                                "count".
    filter_threshold_percentage  0.5                            float 0.0-1.0
                                                                (when filter_method=
                                                                "percentage").
    filter_threshold_count       None                           int >= 1; REQUIRED
                                                                when filter_method=
                                                                "count".
    filter_valid_values_logic    "at least one condition"       "at least one
                                                                condition" |
                                                                "all conditions" |
                                                                "full experiment".
    limma_trend                  True                           True | False.
    robust_empirical_bayes       True                           True | False.
    fit_separate_models          True                           True | False.
    control_variables            None                           list of {column,
                                                                type: categorical|
                                                                numerical}.

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    BEFORE calling this tool:
      Use load_metadata_from_csv to read sample_metadata from the user's CSV.
      NEVER construct sample_metadata manually — sample names must be verbatim.

    Errors:
      - ValueError: not exactly 1 input dataset; empty / malformed
        condition_comparisons; bad species / entity_type;
        filter_method="count" without filter_threshold_count.
      - APIError 422: input dataset is not an INTENSITY dataset, or fewer than
        100 genes shared with the gene-set library.

    Guardrails:
      - input_dataset_ids are DATASET ids, not upload ids.
    """
    sm = SampleMetadata(data=sample_metadata)

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

    control_variables_payload: Optional[Dict[str, List[Dict[str, str]]]] = None
    if control_variables is not None:
        control_variables_payload = {"control_variables": control_variables}

    dataset_id = GseaDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        sample_metadata=sm,
        condition_column=condition_column,
        condition_comparisons=condition_comparisons,
        species=species,
        entity_type=entity_type,
        sets=sets,  # type: ignore[arg-type]
        filter_values_criteria=filter_values_criteria,
        filter_valid_values_logic=filter_valid_values_logic,
        limma_trend=limma_trend,
        robust_empirical_bayes=robust_empirical_bayes,
        fit_separate_models=fit_separate_models,
        control_variables=control_variables_payload,
    ).run(get_client())
    return f"GSEA pipeline started. Dataset ID: {dataset_id}"
