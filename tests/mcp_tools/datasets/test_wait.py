"""Tests for wait_for_dataset."""

from unittest.mock import MagicMock, patch

from mcp_tools.datasets import wait_for_dataset

from .conftest import mock_dataset


class TestWaitForDataset:
    def test_completes_successfully(self):
        mock_client = MagicMock()
        mock_client.datasets.wait_until_complete.return_value = mock_dataset(
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
            mock_dataset(id="ds-1", state="RUNNING")
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
