"""Tests for generate_pairwise_comparisons and run_pairwise_comparison."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import generate_pairwise_comparisons, run_pairwise_comparison

from .conftest import INTENSITY_ID, OUTPUT_ID, SAMPLE_METADATA, patch_pipeline_client


class TestGeneratePairwiseComparisons:
    def test_vs_control(self):
        pairs = json.loads(
            generate_pairwise_comparisons(
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                control="ctrl",
            )
        )
        assert ["treated", "ctrl"] in pairs
        assert all(p[1] == "ctrl" for p in pairs)

    def test_all_pairwise(self):
        pairs = json.loads(
            generate_pairwise_comparisons(
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
            )
        )
        assert len(pairs) == 1
        assert ["treated", "ctrl"] in pairs

    def test_single_condition_returns_empty(self):
        one_condition = [
            ["sample_name", "condition"],
            ["s1", "ctrl"],
            ["s2", "ctrl"],
        ]
        pairs = json.loads(
            generate_pairwise_comparisons(
                sample_metadata=one_condition,
                condition_column="condition",
            )
        )
        assert pairs == []


class TestRunPairwiseComparison:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Pairwise",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_with_control_variables(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Pairwise",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                control_variables=[{"column": "dose", "type": "numerical"}],
            )

        call_args = mock_client.datasets.create.call_args[0][0]
        cv = call_args.job_run_params["control_variables"]
        assert cv == {"control_variables": [{"column": "dose", "type": "numerical"}]}
