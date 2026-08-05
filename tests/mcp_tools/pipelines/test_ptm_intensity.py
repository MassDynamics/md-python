"""Tests for the run_ptm_intensity_table MCP tool."""

from unittest.mock import MagicMock

import pytest

from mcp_tools.pipelines import run_ptm_intensity_table

from .conftest import patch_pipeline_client

OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"
PEPTIDE = "11111111-1111-1111-1111-111111111111"


def _client():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID
    return mock_client


class TestRunPtmIntensityTable:
    def test_basic_run_returns_dataset_id_sentinel(self):
        mock_client = _client()

        with patch_pipeline_client(mock_client):
            result = run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="PTM rollup",
            )

        assert result == f"PTM intensity table started. Dataset ID: {OUTPUT_ID}"
        mock_client.datasets.create.assert_called_once()

    def test_defaults_reproduce_upload_time_rollup(self):
        """Defaults must match flows/ptm_intensity_dataset_types.py exactly.

        The flow's contract is that untouched defaults reproduce the
        upload-time PTM tables; drifting any default here silently breaks
        that guarantee.
        """
        mock_client = _client()

        with patch_pipeline_client(mock_client):
            run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="PTM rollup",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert sent.job_slug == "ptm_intensity_table"
        params = sent.job_run_params
        assert params["entity_type"] == "peptide"
        assert params["loc_probability_threshold"] == 0.0
        assert params["modification_types"] == []
        assert params["summarization_method"] == "sum"
        assert params["include_imputed"] is False
        assert params["impute_missing_values"] is True
        assert params["flanking_window"] == 20
        # output_dataset_type is server-derived from the job slug, not a param.
        assert "output_dataset_type" not in params

    def test_all_arguments_are_forwarded(self):
        mock_client = _client()

        with patch_pipeline_client(mock_client):
            run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="Phospho only, strict",
                loc_probability_threshold=0.75,
                modification_types=["Phosphorylation", "TMTpro"],
                summarization_method="median",
                include_imputed=True,
                impute_missing_values=False,
                flanking_window=7,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["loc_probability_threshold"] == 0.75
        assert params["modification_types"] == ["Phosphorylation", "TMTpro"]
        assert params["summarization_method"] == "median"
        assert params["include_imputed"] is True
        assert params["impute_missing_values"] is False
        assert params["flanking_window"] == 7

    def test_input_dataset_is_passed_through_as_uuid(self):
        mock_client = _client()

        with patch_pipeline_client(mock_client):
            run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="PTM rollup",
            )

        sent = mock_client.datasets.create.call_args[0][0]
        assert [str(x) for x in sent.input_dataset_ids] == [PEPTIDE]
        assert sent.name == "PTM rollup"


class TestValidation:
    def test_rejects_more_than_one_input_dataset(self):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="exactly 1 input dataset"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE, OUTPUT_ID],
                    dataset_name="PTM rollup",
                )
        mock_client.datasets.create.assert_not_called()

    def test_rejects_empty_input_dataset_ids(self):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="cannot be empty"):
                run_ptm_intensity_table(
                    input_dataset_ids=[],
                    dataset_name="PTM rollup",
                )

    def test_rejects_non_peptide_entity_type(self):
        """Only peptide datasets carry PTM_sites, so nothing else can roll up."""
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="entity_type must be 'peptide'"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="PTM rollup",
                    entity_type="protein",
                )
        mock_client.datasets.create.assert_not_called()

    @pytest.mark.parametrize("threshold", [-0.1, 1.1])
    def test_rejects_threshold_out_of_range(self, threshold):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="between 0.0 and 1.0"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="PTM rollup",
                    loc_probability_threshold=threshold,
                )

    def test_rejects_unsupported_modification_type(self):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="Unsupported modification_types"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="PTM rollup",
                    modification_types=["Phospho"],  # internal name, not the label
                )

    def test_accepts_all_eight_supported_modification_types(self):
        """The form offers all eight; only the module dropdowns show four."""
        mock_client = _client()
        every = [
            "Acetylation",
            "Oxidation",
            "Phosphorylation",
            "Carbamidomethylation",
            "Deamidation",
            "Pyro-glu",
            "Met-loss",
            "TMTpro",
        ]
        with patch_pipeline_client(mock_client):
            run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="PTM rollup",
                modification_types=every,
            )
        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["modification_types"] == every

    def test_rejects_unknown_summarisation_method(self):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="summarization_method must be"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="PTM rollup",
                    summarization_method="mean",
                )

    @pytest.mark.parametrize("method", ["sum", "median", "tmp"])
    def test_accepts_each_summarisation_method(self, method):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            run_ptm_intensity_table(
                input_dataset_ids=[PEPTIDE],
                dataset_name="PTM rollup",
                summarization_method=method,
            )
        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["summarization_method"] == method

    @pytest.mark.parametrize("window", [-1, 31])
    def test_rejects_flanking_window_out_of_range(self, window):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="flanking_window must be between"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="PTM rollup",
                    flanking_window=window,
                )

    def test_rejects_missing_dataset_name(self):
        mock_client = _client()
        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="dataset_name is required"):
                run_ptm_intensity_table(
                    input_dataset_ids=[PEPTIDE],
                    dataset_name="",
                )
