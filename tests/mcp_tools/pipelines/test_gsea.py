"""Tests for the run_gsea MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_gsea

from .conftest import SAMPLE_METADATA, patch_pipeline_client

OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"
INTENSITY = "11111111-1111-1111-1111-111111111111"


class TestRunGsea:
    def test_basic_run_returns_dataset_id_sentinel(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_gsea(
                input_dataset_ids=[INTENSITY],
                dataset_name="GSEA run",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                species="Human",
            )

        assert result == f"GSEA pipeline started. Dataset ID: {OUTPUT_ID}"
        mock_client.datasets.create.assert_called_once()

    def test_sends_camera_gsea_slug_and_default_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_gsea(
                input_dataset_ids=[INTENSITY],
                dataset_name="GSEA run",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                species="Human",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert sent.job_slug == "camera_gsea"
        params = sent.job_run_params
        assert params["entity_type"] == "protein"
        assert params["species"] == "Human"
        assert params["sets"] == [
            "GO - Biological Process",
            "GO - Cellular Component",
            "GO - Molecular Function",
        ]
        assert params["condition_comparisons"] == {
            "condition_comparison_pairs": [["treated", "ctrl"]]
        }
        assert params["filter_values_criteria"] == {
            "method": "percentage",
            "filter_threshold_percentage": 0.5,
        }
        # output_dataset_type is server-derived, not a params member.
        assert "output_dataset_type" not in params

    def test_count_filter_and_control_variables(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_gsea(
                input_dataset_ids=[INTENSITY],
                dataset_name="GSEA tuned",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                species="Mouse",
                entity_type="gene",
                sets=["Reactome"],
                filter_method="count",
                filter_threshold_count=3,
                control_variables=[{"column": "batch", "type": "categorical"}],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["entity_type"] == "gene"
        assert params["sets"] == ["Reactome"]
        assert params["filter_values_criteria"] == {
            "method": "count",
            "filter_threshold_count": 3,
        }
        assert params["control_variables"] == {
            "control_variables": [{"column": "batch", "type": "categorical"}]
        }

    def test_count_filter_without_count_rejected(self):
        """Local validation now returns the prose "Error: ..." envelope."""
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            result = run_gsea(
                input_dataset_ids=[INTENSITY],
                dataset_name="GSEA bad",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                species="Human",
                filter_method="count",
            )
        assert result.startswith("Error: ")
        assert "filter_threshold_count" in result
        mock_client.datasets.create.assert_not_called()

    def test_bad_species_rejected(self):
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            result = run_gsea(
                input_dataset_ids=[INTENSITY],
                dataset_name="GSEA bad",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                species="human",
            )
        assert result.startswith("Error: ")
        assert "species" in result
        mock_client.datasets.create.assert_not_called()


class TestRunGseaSetsValidation:
    """The `sets` enum is species-conditional and MUST NOT be silently dropped."""

    def _run(self, mock_client, **overrides):
        kwargs = dict(
            input_dataset_ids=[INTENSITY],
            dataset_name="GSEA",
            sample_metadata=SAMPLE_METADATA,
            condition_column="condition",
            condition_comparisons=[["treated", "ctrl"]],
            species="Human",
        )
        kwargs.update(overrides)
        with patch_pipeline_client(mock_client):
            return run_gsea(**kwargs)  # type: ignore[arg-type]

    def test_hallmark_shorthand_returns_error_envelope_and_never_submits(self):
        """The confirmed live bug: 4 sets requested, 3 ran, no warning."""
        mock_client = MagicMock()
        result = self._run(
            mock_client,
            sets=[
                "GO - Biological Process",
                "GO - Cellular Component",
                "GO - Molecular Function",
                "Hallmark",
            ],
        )

        assert result.startswith("Error: ")
        assert "'Hallmark'" in result
        assert "MSigDB-H (hallmark gene sets)" in result  # the targeted hint
        assert "Human" in result
        mock_client.datasets.create.assert_not_called()

    def test_human_only_value_under_mouse_returns_error_with_mouse_list(self):
        mock_client = MagicMock()
        result = self._run(
            mock_client,
            species="Mouse",
            sets=["MSigDB-C2 (curated gene sets)"],
        )

        assert result.startswith("Error: ")
        assert "MSigDB-M2 (curated gene sets)" in result
        assert "'Mouse'" in result
        mock_client.datasets.create.assert_not_called()

    def test_valid_msigdb_value_is_submitted_verbatim(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID
        result = self._run(
            mock_client,
            sets=["MSigDB-H (hallmark gene sets)", "GO - Biological Process"],
        )

        assert result == f"GSEA pipeline started. Dataset ID: {OUTPUT_ID}"
        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["sets"] == [
            "MSigDB-H (hallmark gene sets)",
            "GO - Biological Process",
        ]

    def test_case_variant_is_normalised_to_the_catalogue_spelling(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID
        self._run(mock_client, sets=["  reactome "])

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["sets"] == ["Reactome"]
