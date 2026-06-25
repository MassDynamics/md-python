"""ORA (Over-Representation Analysis) pipeline tool."""

from typing import List, Optional

from md_python.models.dataset_builders import OraDataset

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_ora(
    input_dataset_ids: List[str],
    dataset_name: str,
    foreground_ids: List[str],
    species: str,
    background: str = "Detected features in this dataset",
    entity_type: str = "protein",
    database: str = "GO - Biological Process",
    custom_background_ids: Optional[List[str]] = None,
    min_gene_set_size: int = 5,
    max_gene_set_size: int = 500,
) -> str:
    """Run an ORA (Over-Representation Analysis) enrichment.

    ORA tests whether a user-supplied list of entities (the FOREGROUND) is
    enriched for any pathway / gene-set in the chosen database, using the
    hypergeometric test with Benjamini-Hochberg correction (clusterProfiler,
    Wu et al. 2021). Output is an ORA dataset with one row per gene set
    (overlap count, fold enrichment, rich factor, p-value, adjusted p-value).

    Returns: prose. Exact string "ORA pipeline started. Dataset ID: <uuid>"
    on success. The "Dataset ID:" sentinel is stable.

    Use this when: the user already has a curated list of entities of
    interest (e.g. the significant hits from a pairwise comparison) and wants
    to know which pathways that list is over-represented in. For a
    threshold-free, whole-ranking test use run_gsea instead.

    Do NOT use this when: there is no foreground list — ORA needs an explicit
    set of foreground entity IDs.

    INPUT REQUIREMENTS:
      * input_dataset_ids: exactly ONE INTENSITY dataset UUID. This is a
        DATASET id (not an upload id).
      * foreground_ids: the entity IDs (of the chosen entity_type) that form
        the foreground tested for over-representation.

    Backend job slug: "ora" (output_dataset_type "ORA"). Parameter defaults /
    bounds / enums are from the live job catalogue (/jobs -> slug "ora",
    MDORAParamsProperties).

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter              Platform default                 Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    species                (required, no default)           "human" | "mouse" |
                                                             "yeast" |
                                                             "chinese_hamster".
    entity_type            "protein"                         "protein" | "gene".
                                                             Type the foreground
                                                             IDs are.
    database               "GO - Biological Process"         Gene-set collection.
                                                             Options depend on
                                                             species (Reactome,
                                                             GO BP/CC/MF, MSigDB
                                                             collections).
    background             "Detected features in              "Detected features in
                            this dataset"                     this dataset"
                                                             (recommended) |
                                                             "Selected Database" |
                                                             "Custom Background
                                                             List".
    custom_background_ids  None                              REQUIRED when
                                                             background="Custom
                                                             Background List".
    min_gene_set_size      5                                 int >= 1. Sets with
                                                             fewer members in the
                                                             background are dropped.
    max_gene_set_size      500                               int >= 1. Sets with
                                                             more members are
                                                             dropped.

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    Errors:
      - ValueError: not exactly 1 input dataset; empty foreground_ids; bad
        species / entity_type / background; background="Custom Background List"
        without custom_background_ids; gene-set size bounds out of range.
      - APIError 422: input dataset is not an INTENSITY dataset, or the
        database is not available for the chosen species.

    Guardrails:
      - input_dataset_ids are DATASET ids, not upload ids.
      - ORA takes a single INTENSITY dataset plus a foreground list.
    """
    dataset_id = OraDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        foreground_ids=foreground_ids,
        species=species,
        background=background,
        entity_type=entity_type,
        database=database,
        custom_background_ids=custom_background_ids,
        min_gene_set_size=min_gene_set_size,
        max_gene_set_size=max_gene_set_size,
    ).run(get_client())
    return f"ORA pipeline started. Dataset ID: {dataset_id}"
