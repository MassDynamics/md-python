"""Tests for describe_entity_type."""

import json

import pytest

from mcp_tools.entity_meta import describe_entity_type


@pytest.mark.parametrize(
    "entity_type",
    ["protein", "peptide", "gene", "metabolite", "ptm"],
)
def test_returns_payload_for_every_known_entity(entity_type):
    result = json.loads(describe_entity_type(entity_type))
    assert result["entity_type"] == entity_type
    for key in (
        "upload_sources",
        "normalisation_methods",
        "imputation_methods",
        "filtration_methods",
        "de_methods",
        "pipelines",
        "notes",
    ):
        assert key in result, f"missing {key} for {entity_type}"
        assert isinstance(result[key], list)
        assert len(result[key]) > 0


def test_unknown_entity_type_returns_error():
    result = json.loads(describe_entity_type("lipid"))
    assert "error" in result
    assert "metabolite" in result["error"]  # one of the valid values


def test_entity_type_is_case_normalised():
    result = json.loads(describe_entity_type("PTM"))
    # Lowercase canonical form is used internally.
    assert result["entity_type"] == "ptm"


def test_gene_de_methods_include_count_engines():
    result = json.loads(describe_entity_type("gene"))
    assert set(result["de_methods"]) == {"limma", "edgeR", "DESeq2"}


def test_non_gene_de_methods_are_limma_only():
    for entity in ("protein", "peptide", "metabolite", "ptm"):
        result = json.loads(describe_entity_type(entity))
        assert result["de_methods"] == ["limma"], (
            f"{entity} should be limma-only per process_r.py — got {result['de_methods']}"
        )


def test_gene_normalisation_methods_include_cpm():
    result = json.loads(describe_entity_type("gene"))
    assert "cpm" in result["normalisation_methods"]


def test_gene_filtration_methods_are_minimum_abundance_only():
    result = json.loads(describe_entity_type("gene"))
    assert set(result["filtration_methods"]) == {"skip", "by minimum abundance"}


def test_ptm_filtration_includes_localization_probability():
    result = json.loads(describe_entity_type("ptm"))
    assert "by ptm localization probability" in result["filtration_methods"]


def test_metabolite_notes_flag_upstream_ni_gap():
    result = json.loads(describe_entity_type("metabolite"))
    combined = " ".join(result["notes"]).lower()
    assert "metabolite" in combined
    assert "upstream" in combined or "422" in combined


def test_metabolite_upload_source_is_md_format_metabolite():
    result = json.loads(describe_entity_type("metabolite"))
    assert result["upload_sources"] == ["md_format_metabolite"]


def test_gene_upload_source_is_md_format_gene():
    result = json.loads(describe_entity_type("gene"))
    assert result["upload_sources"] == ["md_format_gene"]
