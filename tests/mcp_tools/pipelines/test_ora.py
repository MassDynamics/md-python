"""Tests for the run_ora MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_ora

from .conftest import patch_pipeline_client

OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"
INTENSITY = "11111111-1111-1111-1111-111111111111"


class TestRunOra:
    def test_basic_run_returns_dataset_id_sentinel(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_ora(
                input_dataset_ids=[INTENSITY],
                dataset_name="ORA run",
                foreground_ids=["P1", "P2"],
                species="human",
            )

        assert result == f"ORA pipeline started. Dataset ID: {OUTPUT_ID}"
        mock_client.datasets.create.assert_called_once()

    def test_sends_ora_slug_and_default_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_ora(
                input_dataset_ids=[INTENSITY],
                dataset_name="ORA run",
                foreground_ids=["P1", "P2"],
                species="human",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert sent.job_slug == "ora"
        assert sent.job_run_params == {
            "entity_type": "protein",
            "foreground_ids": ["P1", "P2"],
            "species": "human",
            "database": "GO - Biological Process",
            "background": "Detected features in this dataset",
            "min_gene_set_size": 5,
            "max_gene_set_size": 500,
        }
        # output_dataset_type is server-derived, not a params member.
        assert "output_dataset_type" not in sent.job_run_params

    def test_custom_background_passed_through(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_ora(
                input_dataset_ids=[INTENSITY],
                dataset_name="ORA custom",
                foreground_ids=["G1"],
                species="mouse",
                entity_type="gene",
                database="Reactome",
                background="Custom Background List",
                custom_background_ids=["G2", "G3"],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["entity_type"] == "gene"
        assert params["database"] == "Reactome"
        assert params["background"] == "Custom Background List"
        assert params["custom_background_ids"] == ["G2", "G3"]

    def test_bad_species_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="species"):
                run_ora(
                    input_dataset_ids=[INTENSITY],
                    dataset_name="ORA bad",
                    foreground_ids=["P1"],
                    species="rat",
                )
        mock_client.datasets.create.assert_not_called()

    def test_empty_foreground_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="foreground_ids"):
                run_ora(
                    input_dataset_ids=[INTENSITY],
                    dataset_name="ORA bad",
                    foreground_ids=[],
                    species="human",
                )
        mock_client.datasets.create.assert_not_called()
