"""Tests for run_anova MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_anova

from .conftest import INTENSITY_ID, OUTPUT_ID, SAMPLE_METADATA, patch_pipeline_client


class TestRunAnova:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_job_slug_is_anova(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )

        dataset = mock_client.datasets.create.call_args[0][0]
        assert dataset.job_slug == "anova"

    def test_experiment_design_in_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert "experiment_design" in params
        assert params["condition_column"] == "condition"
        assert params["comparisons_type"] == "all"
        assert params["limma_trend"] is True
        assert params["robust_empirical_bayes"] is True

    def test_condition_comparisons_passed_when_provided(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                comparisons_type="custom",
                condition_comparisons=[["treated", "control"]],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["comparisons_type"] == "custom"
        # Source-of-truth: MDFlexiComparisons ConditionComparisons model wraps
        # the list as {"condition_comparison_pairs": [...]}.
        assert params["condition_comparisons"] == {
            "condition_comparison_pairs": [["treated", "control"]],
        }

    def test_condition_comparisons_absent_for_all_type(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert "condition_comparisons" not in params

    def test_custom_filter_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                filter_threshold_percentage=0.7,
                limma_trend=False,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["filter_values_criteria"]["filter_threshold_percentage"] == 0.7
        assert params["limma_trend"] is False


class TestRunAnovaDeMethod:
    """Same wire-format contract as pairwise: entity-keyed de_method."""

    def test_default_emits_de_method_protein_limma(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["de_method_protein"] == "limma"
        assert "de_method" not in params  # flat field would be silently dropped

    def test_gene_edger_emits_companion(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ANOVA gene",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                entity_type="gene",
                de_method="edgeR",
                edger_norm_method="RLE",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["de_method_gene"] == "edgeR"
        assert params["edger_norm_method"] == "RLE"

    def test_gene_deseq2_with_apeglm_emits_seed(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ANOVA gene DESeq2",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                entity_type="gene",
                de_method="DESeq2",
                deseq2_lfc_shrinkage="apeglm",
                deseq2_alpha=0.1,
                apeglm_seed=42,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["de_method_gene"] == "DESeq2"
        assert params["deseq2_lfc_shrinkage"] == "apeglm"
        assert params["deseq2_alpha"] == 0.1
        assert params["apeglm_seed"] == 42

    @pytest.mark.parametrize("entity", ["protein", "peptide", "metabolite", "ptm"])
    def test_rejects_edger_for_non_gene_entity(self, entity):
        with pytest.raises(ValueError, match="de_method 'edgeR' not allowed"):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="x",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                entity_type=entity,
                de_method="edgeR",
            )

    @pytest.mark.parametrize("entity", ["protein", "peptide", "metabolite", "ptm"])
    def test_rejects_deseq2_for_non_gene_entity(self, entity):
        with pytest.raises(ValueError, match="de_method 'DESeq2' not allowed"):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="x",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                entity_type=entity,
                de_method="DESeq2",
            )
