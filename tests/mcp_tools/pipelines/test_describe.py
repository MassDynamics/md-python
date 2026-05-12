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

    def test_pairwise_supports_gene_entity_type(self):
        result = json.loads(describe_pipeline("pairwise_comparison"))
        valid = result["parameters"]["entity_type"]["valid_values"]
        assert valid == ["protein", "peptide", "gene"]

    def test_anova_supports_gene_entity_type(self):
        result = json.loads(describe_pipeline("anova"))
        valid = result["parameters"]["entity_type"]["valid_values"]
        assert valid == ["protein", "peptide", "gene"]

    def test_normalisation_imputation_has_entity_type(self):
        result = json.loads(describe_pipeline("normalisation_imputation"))
        assert "entity_type" in result["parameters"]
        assert "protein" in result["parameters"]["entity_type"]["valid_values"]

    def test_anova_schema_present(self):
        result = json.loads(describe_pipeline("anova"))
        assert "parameters" in result
        assert "condition_column" in result["parameters"]
        assert "comparisons_type" in result["parameters"]

    def test_unknown_slug_returns_error(self):
        result = describe_pipeline("nonexistent_job")
        assert "Unknown job_slug" in result
        assert "nonexistent_job" in result
