"""Tests for run_normalisation_imputation."""

from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import run_normalisation_imputation

from .conftest import INTENSITY_ID, OUTPUT_ID, patch_pipeline_client


class TestRunNormalisationImputation:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="median",
                imputation_method="mnar",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_entity_type_at_top_level(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="median",
                imputation_method="mnar",
                entity_type="peptide",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        # Flat v2 schema: entity_type at top level, methods as scalar strings.
        assert params["entity_type"] == "peptide"
        assert params["normalisation_methods_proteomics"] == "median"
        assert params["imputation_methods"] == "mnar"

    def test_extra_params_merged(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="quantile",
                imputation_method="knn",
                normalisation_extra_params={"reference": "global"},
                imputation_extra_params={"k": 5},
            )

        call_args = mock_client.datasets.create.call_args[0][0]
        params = call_args.job_run_params
        assert params["normalisation_methods_proteomics"] == "quantile"
        assert params["imputation_methods"] == "knn"
        assert params["reference"] == "global"
        assert params["k"] == 5
