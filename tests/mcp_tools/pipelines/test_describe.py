"""Tests for describe_pipeline."""

import json

from mcp_tools.pipelines import describe_pipeline


class TestDescribePipeline:
    def test_known_slug_returns_json(self):
        result = json.loads(describe_pipeline("dose_response"))
        assert "parameters" in result
        assert "normalise" in result["parameters"]
        assert result["parameters"]["normalise"]["valid_values"] == [
            "none",
            "sum",
            "median",
        ]

    def test_all_slugs_have_required_and_parameters(self):
        for slug in (
            "normalisation_imputation",
            "dose_response",
            "pairwise_comparison",
        ):
            result = json.loads(describe_pipeline(slug))
            assert "required" in result, f"missing 'required' for {slug}"
            assert "parameters" in result, f"missing 'parameters' for {slug}"

    def test_normalisation_imputation_valid_methods(self):
        result = json.loads(describe_pipeline("normalisation_imputation"))
        norm_vals = result["parameters"]["normalisation_method"]["valid_values"]
        imp_vals = result["parameters"]["imputation_method"]["valid_values"]
        assert "median" in norm_vals
        assert "quantile" in norm_vals
        assert "skip" in norm_vals
        assert "mnar" in imp_vals
        assert "knn" in imp_vals
        assert "global_median" in imp_vals

    def test_normalisation_imputation_v3_methods_exposed(self):
        """Phase B: schema lists every new method/sub-technique/filtration option."""
        result = json.loads(describe_pipeline("normalisation_imputation"))
        norm = result["parameters"]["normalisation_method"]
        imp = result["parameters"]["imputation_method"]
        filt = result["parameters"]["filtration_method"]

        # Canonical (spaced) wire-form values.
        assert "batch correction" in norm["valid_values"]
        assert "sum" in norm["valid_values"]

        # Imputation: knn_tn and mindet are present.
        assert "knn_tn" in imp["valid_values"]
        assert "mindet" in imp["valid_values"]

        # Filtration entity-keyed values.
        assert "by missing values" in filt["valid_values"]
        assert "by minimum abundance" in filt["valid_values"]
        assert "by ptm localization probability" in filt["valid_values"]

        per_entity = filt["valid_values_per_entity_type"]
        assert "by missing values" in per_entity["protein"]
        assert "by missing values" in per_entity["peptide"]
        assert "by ptm localization probability" in per_entity["peptide"]
        assert "by minimum abundance" in per_entity["gene"]

        # Batch-correction sub-techniques.
        bc_tech = norm["method_params"]["batch correction"][
            "batch_correction_technique"
        ]
        assert "combat" in bc_tech["valid_values"]
        assert "combat seq" in bc_tech["valid_values"]
        assert "limma remove batch effect" in bc_tech["valid_values"]
        assert "combat seq" in bc_tech["valid_values_per_entity_type"]["gene"]
        # Combat seq must NOT be offered for protein/peptide.
        assert "combat seq" not in bc_tech["valid_values_per_entity_type"]["protein"]
        assert "combat seq" not in bc_tech["valid_values_per_entity_type"]["peptide"]

        # filtration_extra_params is a top-level parameter.
        assert "filtration_extra_params" in result["parameters"]

    def test_pairwise_supports_all_entity_types(self):
        result = json.loads(describe_pipeline("pairwise_comparison"))
        valid = result["parameters"]["entity_type"]["valid_values"]
        # Wire format is lowercase; ptm + metabolite confirmed against live
        # job_run_params 2026-05-27.
        assert valid == ["protein", "peptide", "gene", "metabolite", "ptm"]

    def test_anova_supports_all_entity_types(self):
        result = json.loads(describe_pipeline("anova"))
        valid = result["parameters"]["entity_type"]["valid_values"]
        assert valid == ["protein", "peptide", "gene", "metabolite", "ptm"]

    def test_normalisation_imputation_has_entity_type(self):
        result = json.loads(describe_pipeline("normalisation_imputation"))
        assert "entity_type" in result["parameters"]
        assert "protein" in result["parameters"]["entity_type"]["valid_values"]

    def test_anova_schema_present(self):
        result = json.loads(describe_pipeline("anova"))
        assert "parameters" in result
        assert "condition_column" in result["parameters"]
        assert "comparisons_type" in result["parameters"]

    def test_camera_gsea_publishes_per_species_sets(self):
        """`sets` must be as discoverable as every other constrained param."""
        result = json.loads(describe_pipeline("camera_gsea"))
        sets = result["parameters"]["sets"]

        per_species = sets["valid_values_per_species"]
        assert sorted(per_species) == ["Chinese hamster", "Human", "Mouse", "Yeast"]
        assert "MSigDB-H (hallmark gene sets)" in per_species["Human"]
        assert "MSigDB-MH (hallmark gene sets)" in per_species["Mouse"]
        # Mouse uses M-prefixes, not the Human C-numbers.
        assert "MSigDB-C2 (curated gene sets)" not in per_species["Mouse"]
        assert "MSigDB-M2 (curated gene sets)" in per_species["Mouse"]
        # Chinese hamster has no Reactome.
        assert "Reactome" not in per_species["Chinese hamster"]

        # valid_values (the union) is present for parity with sibling params.
        assert "MSigDB-H (hallmark gene sets)" in sets["valid_values"]
        assert "Hallmark" not in sets["valid_values"]
        assert sets["default"] == [
            "GO - Biological Process",
            "GO - Cellular Component",
            "GO - Molecular Function",
        ]

    def test_camera_gsea_constrained_params_all_have_valid_values(self):
        params = json.loads(describe_pipeline("camera_gsea"))["parameters"]
        for name in (
            "species",
            "entity_type",
            "sets",
            "filter_method",
            "filter_valid_values_logic",
        ):
            assert params[name].get("valid_values"), f"{name} publishes no valid_values"

    def test_ora_has_no_sets_param(self):
        """ORA's collection param is `database` (single), not a `sets` list."""
        params = json.loads(describe_pipeline("ora"))["parameters"]
        assert "sets" not in params
        assert "database" in params

    def test_unknown_slug_returns_error(self):
        result = describe_pipeline("nonexistent_job")
        assert "Unknown job_slug" in result
        assert "nonexistent_job" in result
