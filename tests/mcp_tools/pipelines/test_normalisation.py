"""Tests for run_normalisation_imputation."""

from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import run_normalisation_imputation

from .conftest import INTENSITY_ID, OUTPUT_ID


class TestRunNormalisationImputation:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="median",
                imputation_method="min_value",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_extra_params_merged(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="quantile",
                imputation_method="knn",
                normalisation_extra_params={"reference": "global"},
                imputation_extra_params={"k": 5},
            )

        call_args = mock_client.datasets.create.call_args[0][0]
        assert (
            call_args.job_run_params["normalisation_methods"]["reference"] == "global"
        )
        assert call_args.job_run_params["imputation_methods"]["k"] == 5
