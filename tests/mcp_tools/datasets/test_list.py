"""Tests for list_jobs and list_datasets."""

from unittest.mock import MagicMock, patch

from mcp_tools.datasets import list_datasets, list_jobs

from .conftest import mock_dataset


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


class TestListDatasets:
    def test_returns_all_datasets(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset("ds-1", "Initial", "INTENSITY", "COMPLETED"),
            mock_dataset("ds-2", "Pairwise", "PAIRWISE_COMPARISON", "PROCESSING"),
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
            mock_dataset("ds-1", "Initial", "INTENSITY", "COMPLETED"),
            mock_dataset("ds-2", "DR Job", "DOSE_RESPONSE", "COMPLETED"),
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123", type_filter="DOSE_RESPONSE")
        assert "ds-2" in result
        assert "ds-1" not in result
        assert "1 dataset(s)" in result

    def test_type_filter_case_insensitive(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset("ds-1", "DR Job", "DOSE_RESPONSE", "COMPLETED"),
        ]
        with patch("mcp_tools.datasets.get_client", return_value=mock_client):
            result = list_datasets("upload-123", type_filter="dose_response")
        assert "ds-1" in result
