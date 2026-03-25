"""Tests for run_dose_response_bulk."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import run_dose_response_bulk

from .conftest import OUTPUT_ID, mock_dr_ds, mock_initial_ds


class TestRunDoseResponseBulk:
    def test_runs_all_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.side_effect = ["dr-id-1", "dr-id-2"]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": "DR A",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
            {
                "upload_id": "upload-1",
                "dataset_name": "DR B",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        assert result[0]["dataset_id"] == "dr-id-1"
        assert result[1]["dataset_id"] == "dr-id-2"

    def test_skips_existing_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dr_ds(dataset_id="existing-id", name="DR A")
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(
                run_dose_response_bulk(
                    [
                        {
                            "upload_id": "upload-1",
                            "dataset_name": "DR A",
                            "sample_names": ["s1"],
                            "control_samples": ["s1"],
                        }
                    ]
                )
            )

        assert result[0]["dataset_id"] == "existing-id"
        assert result[0]["skipped"] is True
        mock_client.datasets.create.assert_not_called()

    def test_caches_initial_dataset_lookup(self):
        """find_initial_dataset is called once per unique upload_id."""
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.side_effect = ["id-1", "id-2", "id-3"]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": f"DR {i}",
                "sample_names": ["s1"],
                "control_samples": ["s1"],
                "if_exists": "run",
            }
            for i in range(3)
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            run_dose_response_bulk(jobs)

        mock_client.datasets.find_initial_dataset.assert_called_once_with("upload-1")

    def test_auto_fetches_and_caches_upload_metadata(self):
        """Upload metadata is fetched once per upload_id and filtered per job."""
        mock_upload = MagicMock()
        mock_upload.sample_metadata.data = [
            ["sample_name", "dose"],
            ["s1", "0"],
            ["s2", "10"],
            ["s3", "20"],
        ]
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_initial_ds()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.side_effect = ["id-1", "id-2"]
        mock_client.uploads.get_by_id.return_value = mock_upload

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": "DR A",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
            {
                "upload_id": "upload-1",
                "dataset_name": "DR B",
                "sample_names": ["s2", "s3"],
                "control_samples": ["s2"],
                "if_exists": "run",
            },
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        mock_client.uploads.get_by_id.assert_called_once_with("upload-1")
        assert result[0]["dataset_id"] == "id-1"
        assert result[1]["dataset_id"] == "id-2"

    def test_captures_errors_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.side_effect = Exception("HTTP 404")

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(
                run_dose_response_bulk(
                    [
                        {
                            "upload_id": "upload-bad",
                            "dataset_name": "DR A",
                            "sample_names": ["s1"],
                            "control_samples": ["s1"],
                        }
                    ]
                )
            )

        assert "error" in result[0]
        assert result[0]["error_code"] == "dataset_not_found"
