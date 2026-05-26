"""describe_entity_type tool — one-stop catalogue per entity_type.

Aggregates the per-entity allow-lists scattered across the dataset builders
and pipeline schemas into a single response, so an LLM driving the MCP can
discover (a) which upload sources land in a given entity, (b) which NI
methods and filtration methods are valid for it, (c) which downstream
pipelines accept it, and (d) which DE methods are available — without
trial-and-error against the backend.

Pure derivation from existing constants; no network calls.
"""

import json

from md_python.models.dataset_builders._methods import (
    _ENTITY_TYPES,
    _GENE_FILTRATION_METHODS,
    _GENE_NORMALISATION_METHODS,
    _IMPUTATION_METHODS,
    _METABOLITE_FILTRATION_METHODS,
    _PEPTIDE_FILTRATION_METHODS,
    _PROTEIN_FILTRATION_METHODS,
    _PROTEOMICS_NORMALISATION_METHODS,
    _PTM_FILTRATION_METHODS,
)

from . import mcp

# Wire-format upload sources that produce datasets of a given entity_type.
# Confirmed against md_python.resources.v2.uploads.ALLOWED_UPLOAD_SOURCES on
# 2026-05-27. The protein/peptide split is implicit in the file content for
# md_format / maxquant / diann_tabular / spectronaut / tims_diann (the same
# source can produce either depending on which file you point it at).
_SOURCES_PER_ENTITY = {
    "protein": [
        "maxquant",
        "diann_tabular",
        "tims_diann",
        "spectronaut",
        "md_format",
    ],
    "peptide": [
        "maxquant",
        "diann_tabular",
        "spectronaut",
        "md_format",
    ],
    "gene": ["md_format_gene"],
    "metabolite": ["md_format_metabolite"],
    "ptm": ["md_format"],
}

# DE methods available per entity_type at the MDFlexiComparisons layer.
# Confirmed by reading PairwiseParamsProperties / ANOVAParamsProperties in
# MDFlexiComparisons/src/md_flexi_comparisons/process_r.py on 2026-05-27 —
# only gene has multiple choices; everything else is hard-pinned to limma.
_DE_METHODS_PER_ENTITY = {
    "protein": ["limma"],
    "peptide": ["limma"],
    "gene": ["limma", "edgeR", "DESeq2"],
    "metabolite": ["limma"],
    "ptm": ["limma"],
}

# Pipelines reachable from each entity_type. Conservative: lists everything
# the validator allows at this MCP layer. Backend may reject combinations
# (e.g. metabolite NI is upstream-gated as of 2026-05-27).
_PIPELINES_PER_ENTITY = {
    "protein": ["normalisation_imputation", "pairwise_comparison", "anova", "dose_response"],
    "peptide": ["normalisation_imputation", "pairwise_comparison", "anova", "dose_response"],
    "gene": ["normalisation_imputation", "pairwise_comparison", "anova"],
    "metabolite": ["normalisation_imputation", "pairwise_comparison", "anova"],
    "ptm": ["normalisation_imputation", "pairwise_comparison", "anova"],
}

_NORM_METHODS_PER_ENTITY = {
    "protein": _PROTEOMICS_NORMALISATION_METHODS,
    "peptide": _PROTEOMICS_NORMALISATION_METHODS,
    "gene": _GENE_NORMALISATION_METHODS,
    "metabolite": _PROTEOMICS_NORMALISATION_METHODS,
    "ptm": _PROTEOMICS_NORMALISATION_METHODS,
}

_FILT_METHODS_PER_ENTITY = {
    "protein": _PROTEIN_FILTRATION_METHODS,
    "peptide": _PEPTIDE_FILTRATION_METHODS,
    "gene": _GENE_FILTRATION_METHODS,
    "metabolite": _METABOLITE_FILTRATION_METHODS,
    "ptm": _PTM_FILTRATION_METHODS,
}

_NOTES_PER_ENTITY = {
    "protein": [
        "Default and most-trodden path. Every NI/pairwise/ANOVA method is "
        "validated and runs end-to-end. md_format protein files require "
        "Imputed=1 for every row with ProteinIntensity=0.",
    ],
    "peptide": [
        "Subset of the protein path — same NI methods, plus the peptide-"
        "specific filter `by ptm localization probability`. md_format peptide "
        "files require Imputed=1 for every row with PeptideIntensity=0.",
    ],
    "gene": [
        "Uses md-converter's gene reader (md_format_gene); Imputed is auto-"
        "derived (NaN or 0 → Imputed=1). NI normalisation adds `cpm`; "
        "filtration is `by minimum abundance` only.",
        "Only entity_type where pairwise / ANOVA accept de_method ∈ "
        "{limma, edgeR, DESeq2}. edgeR and DESeq2 carry companion params "
        "(edger_norm_method; deseq2_lfc_shrinkage, deseq2_alpha, apeglm_seed).",
        "Gene + limma forces fit_separate_models=False server-side regardless.",
    ],
    "metabolite": [
        "Upload path is supported (source='md_format_metabolite'); Imputed is "
        "REQUIRED and validated 0/1 by md-converter.",
        "NI pipeline (md-converter intensity_imputation) currently does NOT "
        "accept entity_type=metabolite — NI submissions may 422 upstream. "
        "Workaround: run pairwise / ANOVA directly against the upload's "
        "INTENSITY dataset; only limma is available.",
    ],
    "ptm": [
        "PTM behaves like a peptide on the wire (it IS a localised peptide); "
        "filtration includes `by ptm localization probability`. Use for "
        "phospho-proteomics dual-file md_format uploads.",
        "Pairwise / ANOVA accept only de_method='limma' — gene-only count "
        "engines are not relevant for PTM intensities.",
    ],
}


@mcp.tool()
def describe_entity_type(entity_type: str) -> str:
    """Return the full per-entity catalogue: sources, methods, pipelines, notes.

    USE THIS BEFORE making decisions like "which normalisation method is valid
    for metabolite?" or "can I run edgeR pairwise on protein?" — the response
    is a single JSON blob that supersedes the older trial-and-error of
    describe_pipeline + read-the-docstring.

    Args:
        entity_type: one of {"protein", "peptide", "gene", "metabolite",
            "ptm"} (lowercase — UI shows "PTM" / "Metabolite" but the wire
            is lowercase).

    Returns JSON:
      {
        "entity_type": "...",
        "upload_sources": [str, ...],
        "normalisation_methods": [str, ...],   # NI step
        "imputation_methods":    [str, ...],   # entity-agnostic
        "filtration_methods":    [str, ...],   # NI step
        "de_methods":            [str, ...],   # pairwise + ANOVA
        "pipelines":             [str, ...],
        "notes":                 [str, ...],
      }

    Returns ``{"error": "..."}`` on unknown entity_type.
    """
    et = (entity_type or "").lower().strip()
    if et not in _ENTITY_TYPES:
        return json.dumps(
            {
                "error": (
                    f"Unknown entity_type '{entity_type}'. "
                    f"Valid: {sorted(_ENTITY_TYPES)}"
                )
            }
        )

    payload = {
        "entity_type": et,
        "upload_sources": _SOURCES_PER_ENTITY[et],
        "normalisation_methods": sorted(_NORM_METHODS_PER_ENTITY[et]),
        "imputation_methods": sorted(_IMPUTATION_METHODS),
        "filtration_methods": sorted(_FILT_METHODS_PER_ENTITY[et]),
        "de_methods": _DE_METHODS_PER_ENTITY[et],
        "pipelines": _PIPELINES_PER_ENTITY[et],
        "notes": _NOTES_PER_ENTITY[et],
    }
    return json.dumps(payload, indent=2)
