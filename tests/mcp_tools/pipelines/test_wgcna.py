"""Tests for the run_wgcna MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_wgcna

from .conftest import SAMPLE_METADATA, patch_pipeline_client

OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"
INTENSITY = "11111111-1111-1111-1111-111111111111"


class TestRunWgcna:
    def test_basic_run_returns_dataset_id_sentinel(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_wgcna(
                input_dataset_ids=[INTENSITY],
                dataset_name="WGCNA run",
            )

        assert result == f"WGCNA pipeline started. Dataset ID: {OUTPUT_ID}"
        mock_client.datasets.create.assert_called_once()

    def test_sends_wgcna_slug_and_default_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_wgcna(
                input_dataset_ids=[INTENSITY],
                dataset_name="WGCNA run",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert sent.job_slug == "wgcna"
        params = sent.job_run_params
        assert params["entity_type"] == "protein"
        assert params["network_type"] == "signed"
        assert params["min_module_size"] == 30
        assert params["soft_power"] is None
        assert params["deep_split"] == 2
        assert params["filter_method"] is None
        # output_dataset_type is server-derived, not a params member.
        assert "output_dataset_type" not in params
        # sub-params and experiment_design omitted by default
        assert "min_fraction" not in params
        assert "experiment_design" not in params

    def test_filter_method_emits_subparams_and_traits(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_wgcna(
                input_dataset_ids=[INTENSITY],
                dataset_name="WGCNA filtered",
                sample_metadata=SAMPLE_METADATA,
                trait_columns=["condition"],
                entity_type="gene",
                network_type="unsigned",
                soft_power=14,
                filter_method="goodSamplesGenes",
                min_fraction=0.75,
                tol=0.001,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["entity_type"] == "gene"
        assert params["network_type"] == "unsigned"
        assert params["soft_power"] == 14
        assert params["filter_method"] == "goodSamplesGenes"
        assert params["min_fraction"] == 0.75
        assert params["tol"] == 0.001
        assert params["trait_columns"] == ["condition"]
        assert params["experiment_design"]["sample_name"] == [
            "s1",
            "s2",
            "s3",
            "s4",
        ]

    def test_bad_network_type_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="network_type"):
                run_wgcna(
                    input_dataset_ids=[INTENSITY],
                    dataset_name="WGCNA bad",
                    network_type="bipartite",
                )
        mock_client.datasets.create.assert_not_called()

    def test_deep_split_out_of_range_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="deep_split"):
                run_wgcna(
                    input_dataset_ids=[INTENSITY],
                    dataset_name="WGCNA bad",
                    deep_split=9,
                )
        mock_client.datasets.create.assert_not_called()
