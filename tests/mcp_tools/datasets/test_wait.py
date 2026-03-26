"""Tests for wait_for_dataset and _fetch_dataset_state."""

from unittest.mock import MagicMock, patch

from mcp_tools.datasets import _fetch_dataset_state, wait_for_dataset

from .conftest import mock_dataset


class TestWaitForDataset:
    def test_completes_successfully(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.return_value = mock_dataset(
            state="COMPLETED"
        )
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
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
            mock_dataset(id="ds-1", state="RUNNING")
        ]
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = wait_for_dataset(
                "upload-123", "ds-1", poll_seconds=1, timeout_seconds=5
            )
        assert "RUNNING" in result
        assert "call wait_for_dataset again" in result

    def test_timeout_when_dataset_not_yet_visible(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.side_effect = TimeoutError("timed out")
        mock_client.datasets.list_by_upload.return_value = []
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = wait_for_dataset(
                "upload-123", "ds-missing", poll_seconds=1, timeout_seconds=5
            )
        assert "call wait_for_dataset again" in result


class TestFetchDatasetState:
    def test_returns_state_when_found(self):
        ds = MagicMock()
        ds.state = "COMPLETED"
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [ds]

        job = {"upload_id": "up-1", "dataset_id": str(ds.id)}
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = _fetch_dataset_state(job)

        assert result["state"] == "COMPLETED"
        assert "error" not in result

    def test_returns_not_found_when_missing(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []

        job = {"upload_id": "up-1", "dataset_id": "ds-missing"}
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = _fetch_dataset_state(job)

        assert result["state"] == "NOT_FOUND"
        assert "error" in result
        assert result["upload_id"] == "up-1"
        assert result["dataset_id"] == "ds-missing"

    def test_returns_fetch_error_on_api_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.side_effect = RuntimeError(
            "network timeout"
        )

        job = {"upload_id": "up-1", "dataset_id": "ds-1"}
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = _fetch_dataset_state(job)

        assert result["state"] == "FETCH_ERROR"
        assert "network timeout" in result["error"]
        assert result["upload_id"] == "up-1"
        assert result["dataset_id"] == "ds-1"

    def test_uses_get_by_id_when_no_upload_id(self):
        ds = MagicMock()
        ds.state = "RUNNING"
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.return_value = ds

        job = {"dataset_id": "ds-1"}
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = _fetch_dataset_state(job)

        mock_client.datasets.get_by_id.assert_called_once_with("ds-1")
        mock_client.datasets.list_by_upload.assert_not_called()
        assert result["state"] == "RUNNING"
        assert result["dataset_id"] == "ds-1"
        assert "upload_id" not in result

    def test_fetch_error_without_upload_id_omits_upload_id(self):
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.side_effect = RuntimeError("not found")

        job = {"dataset_id": "ds-bad"}
        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = _fetch_dataset_state(job)

        assert result["state"] == "FETCH_ERROR"
        assert "upload_id" not in result
        assert result["dataset_id"] == "ds-bad"
