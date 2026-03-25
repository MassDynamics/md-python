"""Tests for run_dose_response and _filter_sample_metadata."""

from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import _filter_sample_metadata, run_dose_response

from .conftest import INTENSITY_ID, OUTPUT_ID, SAMPLE_METADATA, patch_pipeline_client


class TestRunDoseResponse:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_dose_response(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Dose Response",
                sample_names=["s1", "s2", "s3", "s4"],
                control_samples=["s1", "s2"],
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_with_metadata_included_in_params(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_dose_response(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Dose Response",
                sample_names=["s1", "s2", "s3", "s4"],
                control_samples=["s1", "s2"],
                sample_metadata=SAMPLE_METADATA,
                dose_column="dose",
            )

        call_args = mock_client.datasets.create.call_args[0][0]
        assert "experiment_design" in call_args.job_run_params


class TestFilterSampleMetadata:
    FULL_META = [
        ["sample_name", "dose", "batch"],
        ["s1", "0", "A"],
        ["s2", "0", "A"],
        ["s3", "10", "B"],
        ["s4", "10", "B"],
    ]

    def test_filters_to_requested_samples(self):
        result = _filter_sample_metadata(self.FULL_META, ["s1", "s3"])
        assert result[0] == ["sample_name", "dose", "batch"]
        assert [row[0] for row in result[1:]] == ["s1", "s3"]

    def test_preserves_header(self):
        result = _filter_sample_metadata(self.FULL_META, ["s2"])
        assert result[0] == self.FULL_META[0]

    def test_returns_all_rows_when_all_match(self):
        result = _filter_sample_metadata(self.FULL_META, ["s1", "s2", "s3", "s4"])
        assert len(result) == 5  # header + 4 rows

    def test_missing_sample_name_column_returns_unfiltered(self):
        no_sn = [["filename", "dose"], ["f1.raw", "0"]]
        assert _filter_sample_metadata(no_sn, ["s1"]) == no_sn

    def test_empty_metadata_returns_empty(self):
        assert _filter_sample_metadata([], ["s1"]) == []
