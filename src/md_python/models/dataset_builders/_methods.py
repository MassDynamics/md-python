"""Validation constants and method-name canonicalisation for the
normalisation / imputation / filtration pipeline builder.

Leaf module — must not import any builder, to keep the package
import graph acyclic.
"""

from typing import Dict, Optional

# Wire-format entity_type values accepted by the platform. Confirmed against
# live pairwise job_run_params on 2026-05-27 — `ptm` and `metabolite` are the
# canonical lowercase strings the backend stores (UI shows "PTM" / "Metabolite"
# but the wire is lowercase).
_ENTITY_TYPES = {"protein", "peptide", "gene", "metabolite", "ptm"}

_PROTEOMICS_NORMALISATION_METHODS = {
    "skip",
    "median",
    "quantile",
    "sum",
    "batch correction",
}
_GENE_NORMALISATION_METHODS = {
    "skip",
    "median",
    "quantile",
    "sum",
    "batch correction",
    "cpm",
}

_IMPUTATION_METHODS = {
    "skip",
    "mnar",
    "global_median",
    "median_by_entity",
    "knn",
    "knn_tn",
    "set to constant",
    "set to missing",
    "mindet",
}

_PROTEIN_FILTRATION_METHODS = {"skip", "by missing values"}
_PEPTIDE_FILTRATION_METHODS = {
    "skip",
    "by missing values",
    "by ptm localization probability",
}
_GENE_FILTRATION_METHODS = {"skip", "by minimum abundance"}
# PTM behaves like a peptide on the wire (it IS a localised peptide), and
# `by ptm localization probability` is the obvious filter for it.
_PTM_FILTRATION_METHODS = {
    "skip",
    "by missing values",
    "by ptm localization probability",
}
# Metabolite NI is upstream-gated by md-converter today (intensity_imputation
# does not accept entity_type=metabolite). We still accept skip + by missing
# values client-side so the validator does not over-reject when md-converter
# eventually catches up.
_METABOLITE_FILTRATION_METHODS = {"skip", "by missing values"}

_BATCH_CORRECTION_TECHNIQUES_PROTEOMICS = {
    "limma remove batch effect",
    "combat",
}
_BATCH_CORRECTION_TECHNIQUES_GENE = {
    "limma remove batch effect",
    "combat",
    "combat seq",
}

_FILTER_VALID_VALUES_CRITERIA = {"percentage", "count"}
_FILTER_VALID_VALUES_LOGIC = {
    "all conditions",
    "at least one condition",
    "full experiment",
}

_KNN_TN_DISTANCE = {"truncation", "correlation"}
_KNN_WEIGHTS = {"uniform", "distance"}

# Legacy underscored input values are accepted for backward compatibility and
# normalised to the converter-canonical (spaced) form on the wire.
_METHOD_ALIAS_MAP: Dict[str, str] = {
    "batch_correction": "batch correction",
    "minimum_abundance": "by minimum abundance",
    "by_minimum_abundance": "by minimum abundance",
    "ptm_localization_probability": "by ptm localization probability",
    "by_ptm_localization_probability": "by ptm localization probability",
    "by_missing_values": "by missing values",
    "limma_remove_batch_effect": "limma remove batch effect",
    "combat_seq": "combat seq",
}


def _canon_method(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return _METHOD_ALIAS_MAP.get(value, value)


def _normalisation_methods_key(entity_type: str) -> str:
    return (
        "normalisation_methods_gene"
        if entity_type == "gene"
        else "normalisation_methods_proteomics"
    )


def _filtration_methods_key(entity_type: str) -> str:
    return f"filtration_methods_{entity_type}"


def _batch_correction_technique_key(entity_type: str) -> str:
    return (
        "batch_correction_technique_gene"
        if entity_type == "gene"
        else "batch_correction_technique_proteomics"
    )


# DE methods accepted by mdFlexiComparisons per entity_type. Confirmed by
# reading PairwiseParamsProperties / ANOVAParamsProperties in
# MDFlexiComparisons/src/md_flexi_comparisons/process_r.py on 2026-05-27 —
# only gene exposes multiple DE engines.
_DE_METHODS_PER_ENTITY: Dict[str, frozenset[str]] = {
    "protein": frozenset({"limma"}),
    "peptide": frozenset({"limma"}),
    "gene": frozenset({"limma", "edgeR", "DESeq2"}),
    "metabolite": frozenset({"limma"}),
    "ptm": frozenset({"limma"}),
}

# Companion-parameter vocabularies for the gene DE engines — the allowed values
# edgeR / DESeq2 accept. Mirror of MDFlexiComparisons process_r.py on
# 2026-05-27: the ``edger_norm_method`` / ``deseq2_lfc_shrinkage`` Literals and
# the ``deseq2_alpha`` / ``apeglm_seed`` numberrange ge/le bounds.
#
# These are the SINGLE source for the allowed values, so a sync check only has
# to compare this file against process_r.py. The per-tool gating in
# ``pairwise.py`` and ``anova.py`` reads these constants but keeps its own
# validation block — the two tools can validate differently or be updated in
# different orders without coupling.
_EDGER_NORM_METHODS: frozenset[str] = frozenset(
    {"TMM", "RLE", "upperquartile", "none"}
)
_DESEQ2_LFC_SHRINKAGE: frozenset[str] = frozenset(
    {"none", "apeglm", "ashr", "normal"}
)
_DESEQ2_ALPHA_RANGE: tuple[float, float] = (0.0, 1.0)  # (ge, le)
_APEGLM_SEED_RANGE: tuple[int, int] = (0, 2147483647)  # (ge, le)


def _de_method_key(entity_type: str) -> str:
    """Wire-format key for the per-entity de_method field.

    The data-set-service / MDFlexiComparisons Pydantic schema stores the DE
    method under an entity-keyed field name (e.g. ``de_method_gene``) and
    picks the right one at runtime via ``When.equals("entity_type", "<v>")``
    gates. A flat ``de_method`` key is silently dropped on the wire.
    """
    return f"de_method_{entity_type}"
