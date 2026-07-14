"""CAMERA GSEA (gene-set enrichment) pipeline tool."""

from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from md_python.models.dataset_builders import GseaDataset
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client
from ._errors import format_validation_error


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
                                  "GO - Cellular Component",     Species-conditional
                                  "GO - Molecular Function"]     enum — see the
                                                                SETS table below.
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

    ══ SETS — THE EXACT LEGAL VALUES, PER SPECIES ══════════════════════════════
    Use these strings VERBATIM. They are long and punctuated; copy them, do not
    paraphrase. Shorthand ("Hallmark", "MSigDB-H", "GO BP") is NOT accepted.

    ⚠ HISTORICAL HAZARD: the backend SILENTLY DROPS a `sets` value it does not
    recognise — the job is accepted, reports COMPLETED, and simply never runs
    that knowledge base. A real run submitted with sets=[...3 GO..., "Hallmark"]
    ran only the three GO sets while its dataset name promised Hallmark. This
    MCP now REJECTS unknown values before submission ("Error: ..."), but the
    correct spelling is still your responsibility.

    Human (14):
      "Reactome", "GO - Biological Process", "GO - Cellular Component",
      "GO - Molecular Function", "MSigDB-H (hallmark gene sets)",
      "MSigDB-C1 (positional gene sets)", "MSigDB-C2 (curated gene sets)",
      "MSigDB-C3 (regulatory target gene sets)",
      "MSigDB-C4 (computational gene sets)", "MSigDB-C5 (ontology gene sets)",
      "MSigDB-C6 (oncogenic signature gene sets)",
      "MSigDB-C7 (immunologic signature gene sets)",
      "MSigDB-C8 (cell type signature gene sets)",
      "MSigDB-C9 (computational perturbation signature gene sets)"

    Mouse (11) — MSigDB prefixes are MH/M1/M2/M3/M5/M7/M8, NOT the Human
    C-numbers. A Human value passed with species="Mouse" is REJECTED:
      "Reactome", "GO - Biological Process", "GO - Cellular Component",
      "GO - Molecular Function", "MSigDB-MH (hallmark gene sets)",
      "MSigDB-M1 (positional gene sets)", "MSigDB-M2 (curated gene sets)",
      "MSigDB-M3 (regulatory target gene sets)",
      "MSigDB-M5 (ontology gene sets)",
      "MSigDB-M7 (immunologic signature gene sets)",
      "MSigDB-M8 (cell type signature gene sets)"

    Yeast (4):
      "Reactome", "GO - Biological Process", "GO - Cellular Component",
      "GO - Molecular Function"

    Chinese hamster (3) — NO Reactome:
      "GO - Biological Process", "GO - Cellular Component",
      "GO - Molecular Function"

    Matching is case-insensitive after whitespace is trimmed, and the value is
    normalised to the spelling above. Nothing else is accepted, and nothing is
    ever dropped. "Hallmark" -> "MSigDB-H (hallmark gene sets)" (Human) /
    "MSigDB-MH (hallmark gene sets)" (Mouse).
    ═══════════════════════════════════════════════════════════════════════════════

    BEFORE calling this tool:
      Use load_metadata_from_csv to read sample_metadata from the user's CSV.
      NEVER construct sample_metadata manually — sample names must be verbatim.

    Errors:
      - "Error: <message>" (prose envelope, NOT an exception): local validation
        failed before submission — not exactly 1 input dataset; empty /
        malformed condition_comparisons; bad species / entity_type; a `sets`
        value that is not a knowledge base of the chosen species (the message
        names the offending value and lists the species' valid sets);
        filter_method="count" without filter_threshold_count. Fix and re-call.
      - APIError 422: input dataset is not an INTENSITY dataset, or fewer than
        100 genes shared with the gene-set library.

    Guardrails:
      - input_dataset_ids are DATASET ids, not upload ids.
    """
    try:
        dataset_id = _submit(
            input_dataset_ids=input_dataset_ids,
            dataset_name=dataset_name,
            sample_metadata=sample_metadata,
            condition_column=condition_column,
            condition_comparisons=condition_comparisons,
            species=species,
            entity_type=entity_type,
            sets=sets,
            filter_method=filter_method,
            filter_threshold_percentage=filter_threshold_percentage,
            filter_threshold_count=filter_threshold_count,
            filter_valid_values_logic=filter_valid_values_logic,
            limma_trend=limma_trend,
            robust_empirical_bayes=robust_empirical_bayes,
            fit_separate_models=fit_separate_models,
            control_variables=control_variables,
        )
    except ValidationError as e:
        # ValidationError subclasses ValueError — catch it first and flatten.
        return f"Error: {format_validation_error(e)}"
    except ValueError as e:
        # Local (pre-submission) validation: bad species / entity_type / sets,
        # missing filter_threshold_count, malformed comparisons. Surfaced as the
        # prose error envelope (same shape as run_normalisation_imputation) so
        # the LLM gets a recovery path instead of an uncaught exception.
        return f"Error: {e}"
    return f"GSEA pipeline started. Dataset ID: {dataset_id}"


def _build_filter_criteria(
    filter_method: str,
    filter_threshold_percentage: float,
    filter_threshold_count: Optional[int],
) -> Dict[str, Any]:
    """Assemble filter_values_criteria; raise ValueError on a bad combination."""
    if filter_method not in {"percentage", "count"}:
        raise ValueError(
            f"filter_method must be 'percentage' or 'count' (got '{filter_method}')"
        )
    if filter_method == "percentage":
        if not 0.0 <= filter_threshold_percentage <= 1.0:
            raise ValueError(
                "filter_threshold_percentage must be between 0 and 1 "
                f"(got {filter_threshold_percentage})"
            )
        return {
            "method": "percentage",
            "filter_threshold_percentage": filter_threshold_percentage,
        }
    if filter_threshold_count is None or filter_threshold_count < 1:
        raise ValueError(
            "filter_threshold_count (int >= 1) is required when filter_method='count'"
        )
    return {"method": "count", "filter_threshold_count": filter_threshold_count}


def _submit(
    *,
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: List[List[str]],
    condition_column: str,
    condition_comparisons: List[List[str]],
    species: str,
    entity_type: str,
    sets: Optional[List[str]],
    filter_method: str,
    filter_threshold_percentage: float,
    filter_threshold_count: Optional[int],
    filter_valid_values_logic: str,
    limma_trend: bool,
    robust_empirical_bayes: bool,
    fit_separate_models: bool,
    control_variables: Optional[List[Dict[str, str]]],
) -> str:
    """Build + submit the GSEA dataset. Raises ValueError on local validation."""
    control_variables_payload: Optional[Dict[str, List[Dict[str, str]]]] = None
    if control_variables is not None:
        control_variables_payload = {"control_variables": control_variables}

    return GseaDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        sample_metadata=SampleMetadata(data=sample_metadata),
        condition_column=condition_column,
        condition_comparisons=condition_comparisons,
        species=species,
        entity_type=entity_type,
        sets=sets,  # type: ignore[arg-type]
        filter_values_criteria=_build_filter_criteria(
            filter_method, filter_threshold_percentage, filter_threshold_count
        ),
        filter_valid_values_logic=filter_valid_values_logic,
        limma_trend=limma_trend,
        robust_empirical_bayes=robust_empirical_bayes,
        fit_separate_models=fit_separate_models,
        control_variables=control_variables_payload,
    ).run(get_client())
