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
    # A PTM dataset is built at upload by every runner that can see modified
    # peptide sequences — not just md_format. Confirmed on 2026-08-06 by
    # reading md-converter: diann_matrix/runner.py, spectronaut/runner.py and
    # md_format/runner.py all write PTM_sites and then call
    # `create_and_check_ptm_table`. Ordered probabilities-capable-first,
    # because that distinction (see _NOTES_PER_ENTITY) is what should drive
    # the choice between them.
    "ptm": ["diann_tabular", "spectronaut", "md_format"],
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
    # `ptm_intensity_table` is peptide-IN / PTM-OUT: it reads the PTM_sites
    # table that only a peptide dataset carries, and emits a PTM dataset. It is
    # therefore reachable FROM peptide, not from ptm.
    "peptide": [
        "normalisation_imputation",
        "pairwise_comparison",
        "anova",
        "dose_response",
        "ptm_intensity_table",
    ],
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
        "filtration includes `by ptm localization probability`.",
        "A PTM dataset is created AT UPLOAD, automatically, whenever the data "
        "contains modified peptides — there is no opt-in flag. It is built by "
        "the diann_tabular, spectronaut and md_format runners alike.",
        "ONLY diann_tabular and spectronaut can supply real PTM localisation "
        "probabilities. md_format has no column for them, so every "
        "probability is set to 1.0 and `by ptm localization probability` "
        "becomes a NO-OP that still looks like a filter. Never advise "
        "converting a DIA-NN or Spectronaut report to md_format in order to "
        "get PTMs — that destroys the localisation data.",
        "Probabilities also require the right export: Spectronaut needs the "
        "PTM-localisation schema (EG.PrecursorId, "
        "EG.PTMLocalizationProbabilities, EG.TotalQuantity) AND Minor "
        "(Peptide) Grouping set to 'Modified Sequence'; DIA-NN needs "
        "report.parquet (v2.0+) alongside the pr_matrix. Without the "
        "pr_matrix, a DIA-NN upload yields no PTM sites at all.",
        "Pairwise / ANOVA accept only de_method='limma' — gene-only count "
        "engines are not relevant for PTM intensities.",
        "Re-running the site rollup with custom settings (summarisation "
        "method, modification subset, probability threshold, imputation, "
        "flanking window) is `run_ptm_intensity_table` — it takes the PEPTIDE "
        "dataset (which carries PTM_sites) as input and emits a new PTM "
        "dataset. With all defaults it reproduces the upload-time PTM tables "
        "exactly, so only call it when the user wants something DIFFERENT.",
        "The threshold filters PTMProbSample (per-sample), NOT PTMProbMax "
        "(best across all samples) — name which you mean. Site GroupIds are "
        "re-assigned on every rollup, so join two rollups on PTMProtein, "
        "never on GroupId.",
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
