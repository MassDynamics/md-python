"""Tests for find_initial_dataset and find_initial_datasets."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import find_initial_dataset, find_initial_datasets

from .conftest import mock_dataset


class TestFindInitialDataset:
    def test_found(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_dataset()
        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = find_initial_dataset("upload-123")
        mock_client.datasets.find_initial_dataset.assert_called_once_with("upload-123")
        assert "ds-1" in result
        assert "Initial dataset found" in result

    def test_not_found(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = None
        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = find_initial_dataset("upload-123")
        assert "No initial" in result


class TestFindInitialDatasets:
    def test_returns_dataset_ids_for_all_uploads(self):
        mock_ds1 = MagicMock()
        mock_ds1.id = "ds-001"
        mock_ds2 = MagicMock()
        mock_ds2.id = "ds-002"
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = [mock_ds1, mock_ds2]

        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-1", "upload-2"]))

        assert result["upload-1"] == {"dataset_id": "ds-001"}
        assert result["upload-2"] == {"dataset_id": "ds-002"}

    def test_records_not_found_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = None
        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-missing"]))
        assert "error" in result["upload-missing"]

    def test_records_exception_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = Exception("HTTP 500")
        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-bad"]))
        assert "HTTP 500" in result["upload-bad"]["error"]

    def test_continues_after_error(self):
        mock_ds = MagicMock()
        mock_ds.id = "ds-ok"
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.side_effect = [
            Exception("not found"),
            mock_ds,
        ]
        with patch("mcp_tools.datasets.find.get_client", return_value=mock_client):
            result = json.loads(find_initial_datasets(["upload-bad", "upload-ok"]))
        assert "error" in result["upload-bad"]
        assert result["upload-ok"] == {"dataset_id": "ds-ok"}
