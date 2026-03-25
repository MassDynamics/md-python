"""Tests for cancel_upload_queue and list_uploads_status."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.uploads import cancel_upload_queue, list_uploads_status


class TestCancelUploadQueue:
    def test_shuts_down_executor_and_creates_replacement(self):
        import mcp_tools.uploads as uploads_module

        original_executor = uploads_module._large_upload_executor
        with patch("mcp_tools.uploads._get_executor", return_value=original_executor):
            with patch.object(original_executor, "shutdown") as mock_shutdown:
                result = cancel_upload_queue()

        mock_shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        assert uploads_module._large_upload_executor is not original_executor
        assert "reset" in result.lower()


class TestListUploadsStatus:
    def test_returns_status_for_each_id(self):
        mock_u1, mock_u2 = MagicMock(), MagicMock()
        mock_u1.name, mock_u1.status, mock_u1.source = (
            "exp1",
            "COMPLETED",
            "diann_tabular",
        )
        mock_u2.name, mock_u2.status, mock_u2.source = "exp2", "PROCESSING", "maxquant"
        mock_client = MagicMock()
        mock_client.uploads.get_by_id.side_effect = [mock_u1, mock_u2]

        with patch("mcp_tools.uploads.get_client", return_value=mock_client):
            result = list_uploads_status(["uid-1", "uid-2"])

        data = json.loads(result)
        assert data["uid-1"]["status"] == "COMPLETED"
        assert data["uid-2"]["status"] == "PROCESSING"

    def test_records_errors_inline(self):
        mock_u1 = MagicMock()
        mock_u1.name, mock_u1.status, mock_u1.source = (
            "exp1",
            "COMPLETED",
            "diann_tabular",
        )
        mock_client = MagicMock()
        mock_client.uploads.get_by_id.side_effect = [mock_u1, Exception("not found")]

        with patch("mcp_tools.uploads.get_client", return_value=mock_client):
            result = list_uploads_status(["uid-ok", "uid-bad"])

        data = json.loads(result)
        assert data["uid-ok"]["status"] == "COMPLETED"
        assert "error" in data["uid-bad"]
