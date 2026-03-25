"""Tests for retry_dataset and delete_dataset."""

from unittest.mock import MagicMock, patch

from mcp_tools.datasets import delete_dataset, retry_dataset


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
