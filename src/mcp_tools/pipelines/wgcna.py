"""WGCNA co-expression network pipeline tool."""

from typing import List, Optional

from md_python.models.dataset_builders import WgcnaDataset
from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_wgcna(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: Optional[List[List[str]]] = None,
    trait_columns: Optional[List[str]] = None,
    entity_type: str = "protein",
    log_transform: bool = True,
    network_type: str = "signed",
    min_module_size: int = 30,
    merge_cut_height: float = 0.25,
    soft_power: Optional[int] = None,
    rsquared_cut: float = 0.9,
    mean_connectivity_cut: int = 100,
    deep_split: int = 2,
    filter_method: Optional[str] = None,
    min_fraction: float = 0.5,
    min_n_samples: int = 4,
    min_n_genes: int = 4,
    min_relative_weight: float = 0.1,
    tol: Optional[float] = None,
) -> str:
    """Run a WGCNA co-expression network analysis.

    WGCNA (PyWGCNA, Rezaie et al. 2023) builds a weighted correlation network
    over entities, detects co-expression modules, summarises each module with
    an eigenentity, and correlates module eigenentities with sample-metadata
    trait columns. Output is a WGCNA dataset with module assignments, module
    eigenentities, module-trait correlations, and the soft-threshold diagnostic.

    Returns: prose. Exact string "WGCNA pipeline started. Dataset ID: <uuid>"
    on success. The "Dataset ID:" sentinel is stable.

    Use this when: the user wants to discover co-expression modules across an
    intensity dataset and relate them to sample traits.

    Do NOT use this when: the input is not complete / numeric — WGCNA drops any
    entity with a 0 or missing value in any sample, so run
    run_normalisation_imputation first.

    INPUT REQUIREMENTS:
      * input_dataset_ids: exactly ONE INTENSITY dataset UUID (a DATASET id).
        Run Normalisation & Imputation upstream so the network is built on
        complete data.
      * sample_metadata (optional): read via load_metadata_from_csv — NEVER
        construct it manually. Required only if you want module-trait
        correlations against trait_columns.

    Backend job slug: "wgcna" (output_dataset_type "WGCNA"). Parameter defaults
    / bounds are from the live job catalogue (/jobs -> slug "wgcna",
    WGCNAParams).

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter              Platform default   Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    entity_type            "protein"          "protein" | "peptide" | "gene".
    trait_columns          None               Metadata columns to correlate
                                              module eigengenes against.
    log_transform          True               Apply log2 before building the
                                              network. Off only if pre-logged.
    network_type           "signed"           "signed" (recommended) |
                                              "unsigned" | "signed hybrid".
    min_module_size        30                 int >= 2. Smallest module kept.
    merge_cut_height       0.25               float 0.0-1.0. Modules closer than
                                              this are merged.
    soft_power             None (auto)        int 1-30 or None. None auto-selects
                                              the lowest power meeting rsquared_cut.
    rsquared_cut           0.9                float 0.0-1.0. Scale-free fit floor
                                              for auto-β.
    mean_connectivity_cut  100                int >= 1. Caps mean connectivity at
                                              the chosen β.
    deep_split             2                  int 0-4. Tree-cut sensitivity.
    filter_method          None               None (skip) | "goodSamplesGenes"
                                              (iterative good-samples/genes filter).
    min_fraction           0.5                float 0.0-1.0. goodSamplesGenes only.
    min_n_samples          4                  int >= 1. goodSamplesGenes only.
    min_n_genes            4                  int >= 1. goodSamplesGenes only.
    min_relative_weight    0.1                float 0.0-1.0. goodSamplesGenes only.
    tol                    None (auto)        float >= 0.0 or None.
                                              goodSamplesGenes only.

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    Errors:
      - ValueError: not exactly 1 input dataset; bad entity_type / network_type
        / filter_method; numeric bounds out of range.
      - APIError 422: input dataset is not an INTENSITY dataset, or too few
        entities survive filtering to build a network.

    Guardrails:
      - input_dataset_ids are DATASET ids, not upload ids.
      - WGCNA needs complete data — run normalisation/imputation first.
    """
    sm: Optional[SampleMetadata] = None
    if sample_metadata is not None:
        sm = SampleMetadata(data=sample_metadata)

    dataset_id = WgcnaDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        sample_metadata=sm,
        trait_columns=trait_columns,
        entity_type=entity_type,
        log_transform=log_transform,
        network_type=network_type,
        min_module_size=min_module_size,
        merge_cut_height=merge_cut_height,
        soft_power=soft_power,
        rsquared_cut=rsquared_cut,
        mean_connectivity_cut=mean_connectivity_cut,
        deep_split=deep_split,
        filter_method=filter_method,
        min_fraction=min_fraction,
        min_n_samples=min_n_samples,
        min_n_genes=min_n_genes,
        min_relative_weight=min_relative_weight,
        tol=tol,
    ).run(get_client())
    return f"WGCNA pipeline started. Dataset ID: {dataset_id}"
