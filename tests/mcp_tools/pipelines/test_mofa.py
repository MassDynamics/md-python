"""Tests for the run_mofa MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_mofa

from .conftest import patch_pipeline_client

OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"
VIEW_A = "11111111-1111-1111-1111-111111111111"
VIEW_B = "22222222-2222-2222-2222-222222222222"


class TestRunMofa:
    def test_basic_run_returns_dataset_id_sentinel(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_mofa(
                input_dataset_ids=[VIEW_A, VIEW_B],
                dataset_name="MOFA integration",
            )

        assert result == f"MOFA+ pipeline started. Dataset ID: {OUTPUT_ID}"
        mock_client.datasets.create.assert_called_once()

    def test_sends_mofa_slug_and_default_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_mofa(
                input_dataset_ids=[VIEW_A, VIEW_B],
                dataset_name="MOFA integration",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert sent.job_slug == "mofa"
        assert sent.job_run_params == {
            "num_factors": 15,
            "convergence_mode": "fast",
            "scale_views": True,
            "center_groups": True,
            "max_iter": 1000,
            "ard_factors": True,
            "drop_factor_threshold": 0.01,
        }

    def test_custom_params_passed_through(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_mofa(
                input_dataset_ids=[VIEW_A, VIEW_B],
                dataset_name="MOFA tuned",
                num_factors=30,
                convergence_mode="slow",
                scale_views=False,
                max_iter=8000,
                drop_factor_threshold=0.0,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["num_factors"] == 30
        assert params["convergence_mode"] == "slow"
        assert params["scale_views"] is False
        assert params["max_iter"] == 8000
        assert params["drop_factor_threshold"] == 0.0

    def test_single_view_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="at least 2"):
                run_mofa(
                    input_dataset_ids=[VIEW_A],
                    dataset_name="MOFA bad",
                )
        mock_client.datasets.create.assert_not_called()

    def test_bad_convergence_mode_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="convergence_mode"):
                run_mofa(
                    input_dataset_ids=[VIEW_A, VIEW_B],
                    dataset_name="MOFA bad",
                    convergence_mode="turbo",
                )
        mock_client.datasets.create.assert_not_called()
