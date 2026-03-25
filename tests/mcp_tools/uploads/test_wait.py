"""Tests for wait_for_upload."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import wait_for_upload


class TestWaitForUpload:
    def test_completes_successfully(self):
        mock_upload = MagicMock()
        mock_upload.__str__ = lambda self: "Upload: done | Status: COMPLETED"
        mock_client = MagicMock()
        mock_client.uploads.wait_until_complete.return_value = mock_upload

        with patch("mcp_tools.uploads.get_client", return_value=mock_client):
            result = wait_for_upload("upload-123", poll_seconds=1, timeout_seconds=60)

        mock_client.uploads.wait_until_complete.assert_called_once_with(
            "upload-123", poll_s=1, timeout_s=60
        )
        assert "COMPLETED" in result

    def test_timeout_returns_current_status_and_retry_instruction(self):
        mock_upload = MagicMock()
        mock_upload.status = "PROCESSING"
        mock_upload.__str__ = lambda self: "Upload: processing"
        mock_client = MagicMock()
        mock_client.uploads.wait_until_complete.side_effect = TimeoutError("timed out")
        mock_client.uploads.get_by_id.return_value = mock_upload

        with patch("mcp_tools.uploads.get_client", return_value=mock_client):
            result = wait_for_upload("upload-123", poll_seconds=1, timeout_seconds=5)

        assert "PROCESSING" in result
        assert "call wait_for_upload again" in result
