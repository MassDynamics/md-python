"""Tests for run_anova MCP tool."""

from unittest.mock import MagicMock

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
        assert params["condition_comparisons"] == [["treated", "control"]]

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
