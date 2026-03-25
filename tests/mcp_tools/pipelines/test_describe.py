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
        assert "min_value" in imp_vals
        assert "knn" in imp_vals

    def test_unknown_slug_returns_error(self):
        result = describe_pipeline("nonexistent_job")
        assert "Unknown job_slug" in result
        assert "nonexistent_job" in result
