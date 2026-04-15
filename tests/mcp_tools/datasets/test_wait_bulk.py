"""Tests for wait_for_datasets_bulk."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import wait_for_datasets_bulk

from .conftest import mock_dataset


class TestWaitForDatasetsBulk:
    def test_all_terminal_returns_immediately(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset(id="ds-1", state="COMPLETED"),
            mock_dataset(id="ds-2", state="COMPLETED"),
        ]

        jobs = [
            {"upload_id": "upload-1", "dataset_id": "ds-1"},
            {"upload_id": "upload-1", "dataset_id": "ds-2"},
        ]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=1, timeout_seconds=5)
            )

        assert result["all_terminal"] is True
        assert result["total"] == 2
        assert result["by_state"]["COMPLETED"] == 2
        assert result["pending"] == []
        assert result["failed"] == []

    def test_mixed_states_returns_pending_list(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset(id="ds-1", state="COMPLETED"),
            mock_dataset(id="ds-2", state="PROCESSING"),
        ]

        jobs = [
            {"upload_id": "upload-1", "dataset_id": "ds-1"},
            {"upload_id": "upload-1", "dataset_id": "ds-2"},
        ]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=60, timeout_seconds=0)
            )

        assert result["all_terminal"] is False
        assert len(result["pending"]) == 1
        assert result["pending"][0]["dataset_id"] == "ds-2"
        assert result["pending"][0]["state"] == "PROCESSING"

    def test_failed_datasets_appear_in_failed_list(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset(id="ds-1", state="FAILED"),
            mock_dataset(id="ds-2", state="COMPLETED"),
        ]

        jobs = [
            {"upload_id": "upload-1", "dataset_id": "ds-1"},
            {"upload_id": "upload-1", "dataset_id": "ds-2"},
        ]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=1, timeout_seconds=5)
            )

        assert result["all_terminal"] is True
        assert len(result["failed"]) == 1
        assert result["failed"][0]["dataset_id"] == "ds-1"

    def test_enforces_job_cap(self):
        jobs = [{"upload_id": "u", "dataset_id": f"ds-{i}"} for i in range(501)]
        result = json.loads(wait_for_datasets_bulk(jobs))
        assert "error" in result
        assert "500" in result["error"]

    def test_not_found_datasets_reach_terminal_state(self):
        """Regression: a job whose dataset_id cannot be found must end up in
        the failed bucket with state NOT_FOUND, not loop forever in pending.
        """
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.return_value = None

        jobs = [{"dataset_id": "ds-missing"}]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=60, timeout_seconds=5)
            )

        assert result["all_terminal"] is True
        assert result["pending"] == []
        assert len(result["failed"]) == 1
        assert result["failed"][0]["dataset_id"] == "ds-missing"
        assert result["failed"][0]["state"] == "NOT_FOUND"

    def test_fetch_error_datasets_reach_terminal_state(self):
        """Regression: a transport error must also be terminal."""
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.side_effect = RuntimeError("boom")

        jobs = [{"dataset_id": "ds-1"}]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=60, timeout_seconds=5)
            )

        assert result["all_terminal"] is True
        assert len(result["failed"]) == 1
        assert result["failed"][0]["state"] == "FETCH_ERROR"

    def test_timeout_returns_current_summary_with_all_terminal_false(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dataset(id="ds-1", state="RUNNING"),
        ]

        jobs = [{"upload_id": "upload-1", "dataset_id": "ds-1"}]

        with patch("mcp_tools.datasets.wait.get_client", return_value=mock_client):
            result = json.loads(
                wait_for_datasets_bulk(jobs, poll_seconds=60, timeout_seconds=0)
            )

        assert result["all_terminal"] is False
        assert result["by_state"].get("RUNNING") == 1
