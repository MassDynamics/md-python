"""Tests for retry_dataset, delete_dataset, and cancel_dataset."""

from unittest.mock import MagicMock, patch

from mcp_tools.datasets import cancel_dataset, delete_dataset, retry_dataset


class TestRetryDataset:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.datasets.retry.return_value = True
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "successfully" in retry_dataset("ds-1")

    def test_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.retry.return_value = False
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "Failed" in retry_dataset("ds-1")


class TestDeleteDataset:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.datasets.delete.return_value = True
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "successfully" in delete_dataset("ds-1")

    def test_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.delete.return_value = False
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "Failed" in delete_dataset("ds-1")


class TestCancelDataset:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.datasets.cancel.return_value = True
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "cancellation requested" in cancel_dataset("ds-1")

    def test_not_processing_returns_error(self):
        mock_client = MagicMock()
        mock_client.datasets.cancel.side_effect = Exception(
            "Failed to cancel dataset: 400 - dataset is not in a cancellable state"
        )
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            result = cancel_dataset("ds-1")
        assert "Failed to cancel" in result
        assert "cancellable" in result

    def test_unknown_server_response(self):
        mock_client = MagicMock()
        mock_client.datasets.cancel.return_value = False
        with patch("mcp_tools.datasets.crud.get_client", return_value=mock_client):
            assert "unknown server response" in cancel_dataset("ds-1")
