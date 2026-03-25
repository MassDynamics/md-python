"""Tests for mcp_tools.datasets."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import (
    delete_dataset,
    find_initial_dataset,
    find_initial_datasets,
    list_datasets,
    list_jobs,
    retry_dataset,
    wait_for_dataset,
)


def _mock_dataset(id="ds-1", name="My Dataset", type="INTENSITY", state="COMPLETED"):
    ds = MagicMock()
    ds.id = id
    ds.name = name
    ds.type = type
    ds.state = state
    ds.__str__ = lambda self: f"Dataset: {name}"
    return ds


class TestListJobs:
    def test_returns_job_slugs(self):
        mock_client = MagicMock()
        mock_client.jobs.list.return_value = [
            {"slug": "normalisation_imputation"},
            {"slug": "pairwise_comparison"},
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_jobs()
        assert "normalisation_imputation" in result
        assert "pairwise_comparison" in result

    def test_empty_returns_message(self):
        mock_client = MagicMock()
        mock_client.jobs.list.return_value = []
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_jobs()
        assert "No jobs" in result


class TestFindInitialDatasets:
    def test_returns_dataset_ids_for_all_uploads(self):
        mock_ds1 = MagicMock()
        mock_ds1.id = "ds-001"
        mock_ds2 = MagicMock()
        mock_ds2.id = "ds-002"
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = [mock_ds1, mock_ds2]

        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-1", "upload-2"]))

        assert result["upload-1"] == {"dataset_id": "ds-001"}
        assert result["upload-2"] == {"dataset_id": "ds-002"}

    def test_records_not_found_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = None

        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-missing"]))

        assert "error" in result["upload-missing"]

    def test_records_exception_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = Exception("HTTP 500")

        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-bad"]))

        assert "error" in result["upload-bad"]
        assert "HTTP 500" in result["upload-bad"]["error"]

    def test_continues_after_error(self):
        mock_ds = MagicMock()
        mock_ds.id = "ds-ok"
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = [
            Exception("not found"),
            mock_ds,
        ]

        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-bad", "upload-ok"]))

        assert "error" in result["upload-bad"]
        assert result["upload-ok"] == {"dataset_id": "ds-ok"}


class TestListDatasets:
    def test_returns_all_datasets(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            _mock_dataset("ds-1", "Initial", "INTENSITY", "COMPLETED"),
            _mock_dataset("ds-2", "Pairwise", "PAIRWISE_COMPARISON", "PROCESSING"),
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123")
        mock_client.datasets.list_by_upload.assert_called_once_with("upload-123")
        assert "2 dataset(s)" in result
        assert "ds-1" in result
        assert "INTENSITY" in result

    def test_empty_returns_message(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123")
        assert "No datasets" in result

    def test_type_filter_restricts_output(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            _mock_dataset("ds-1", "Initial", "INTENSITY", "COMPLETED"),
            _mock_dataset("ds-2", "DR Job", "DOSE_RESPONSE", "COMPLETED"),
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123", type_filter="DOSE_RESPONSE")
        assert "ds-2" in result
        assert "ds-1" not in result
        assert "1 dataset(s)" in result

    def test_type_filter_case_insensitive(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            _mock_dataset("ds-1", "DR Job", "DOSE_RESPONSE", "COMPLETED"),
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123", type_filter="dose_response")
        assert "ds-1" in result


class TestFindInitialDataset:
    def test_found(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = _mock_dataset()
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = find_initial_dataset("upload-123")
        mock_client.datasets.find_initial_dataset.assert_called_once_with("upload-123")
        assert "ds-1" in result
        assert "Initial dataset found" in result

    def test_not_found(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = None
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = find_initial_dataset("upload-123")
        assert "No initial" in result


class TestWaitForDataset:
    def test_completes_successfully(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.return_value = _mock_dataset(
            state="COMPLETED"
        )
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = wait_for_dataset(
                "upload-123", "ds-1", poll_seconds=1, timeout_seconds=60
            )
        mock_client.datasets.wait_until_complete.assert_called_once_with(
            "upload-123", "ds-1", poll_s=1, timeout_s=60
        )
        assert "Dataset: My Dataset" in result

    def test_timeout_returns_current_state_and_retry_instruction(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.side_effect = TimeoutError("timed out")
        mock_client.datasets.list_by_upload.return_value = [
            _mock_dataset(id="ds-1", state="RUNNING")
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = wait_for_dataset(
                "upload-123", "ds-1", poll_seconds=1, timeout_seconds=5
            )
        assert "RUNNING" in result
        assert "call wait_for_dataset again" in result

    def test_timeout_when_dataset_not_yet_visible(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.side_effect = TimeoutError("timed out")
        mock_client.datasets.list_by_upload.return_value = []
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = wait_for_dataset(
                "upload-123", "ds-missing", poll_seconds=1, timeout_seconds=5
            )
        assert "call wait_for_dataset again" in result


class TestRetryDataset:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.datasets.retry.return_value = True
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            assert "successfully" in retry_dataset("ds-1")

    def test_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.retry.return_value = False
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            assert "Failed" in retry_dataset("ds-1")


class TestDeleteDataset:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.datasets.delete.return_value = True
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            assert "successfully" in delete_dataset("ds-1")

    def test_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.delete.return_value = False
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            assert "Failed" in delete_dataset("ds-1")
