"""Validation constants and method-name canonicalisation for the
normalisation / imputation / filtration pipeline builder.

Leaf module — must not import any builder, to keep the package
import graph acyclic.
"""

from typing import Dict, Optional

_ENTITY_TYPES = {"protein", "peptide", "gene"}

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
