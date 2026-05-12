"""Tests for run_dose_response_from_upload."""

from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import run_dose_response_from_upload

from .conftest import OUTPUT_ID, mock_dr_ds, mock_initial_ds, patch_pipeline_client


class TestRunDoseResponseFromUpload:
    def test_finds_dataset_and_runs(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2", "s3"],
                control_samples=["s1"],
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_skips_existing_job(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dr_ds(dataset_id="existing-dr-id", name="My DR")
        ]

        with patch_pipeline_client(mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2"],
                control_samples=["s1"],
                if_exists="skip",
            )

        assert "existing-dr-id" in result
        assert "skipped" in result.lower()
        mock_client.datasets.create.assert_not_called()

    def test_runs_when_if_exists_is_run(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [mock_dr_ds(name="My DR")]
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2"],
                control_samples=["s1"],
                if_exists="run",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_auto_fetches_metadata_from_upload(self):
        """When sample_metadata is omitted, it is fetched from the upload record."""
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.create.return_value = OUTPUT_ID

        mock_upload = MagicMock()
        mock_upload.sample_metadata.data = [
            ["sample_name", "dose"],
            ["s1", "0"],
            ["s2", "0"],
            ["s3", "10"],
        ]
        mock_client.uploads.get_by_id.return_value = mock_upload

        with patch_pipeline_client(mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s3"],
                control_samples=["s1"],
            )

        assert OUTPUT_ID in result
        # experiment_design is serialised as {column_name: [values...]}
        passed_meta = mock_client.datasets.create.call_args[0][0].job_run_params[
            "experiment_design"
        ]
        assert "s1" in passed_meta["sample_name"]
        assert "s3" in passed_meta["sample_name"]
        assert "s2" not in passed_meta["sample_name"]

    def test_no_initial_dataset_returns_error(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.return_value = None

        with patch_pipeline_client(mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-missing",
                dataset_name="My DR",
                sample_names=["s1"],
                control_samples=["s1"],
            )

        assert "Error" in result
        assert "upload-missing" in result
