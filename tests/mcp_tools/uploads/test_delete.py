"""Tests for delete_upload."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import delete_upload


class TestDeleteUpload:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.uploads.delete.return_value = True
        with patch("mcp_tools.uploads.delete.get_client", return_value=mock_client):
            result = delete_upload("upload-1")
        assert "successfully" in result
        mock_client.uploads.delete.assert_called_once_with("upload-1")

    def test_409_conflict_returns_friendly_message(self):
        mock_client = MagicMock()
        mock_client.uploads.delete.side_effect = Exception(
            "Failed to delete upload: 409 - Upload has associated datasets"
        )
        with patch("mcp_tools.uploads.delete.get_client", return_value=mock_client):
            result = delete_upload("upload-1")
        assert "associated datasets" in result
        assert "delete_dataset" in result

    def test_generic_error_propagated(self):
        mock_client = MagicMock()
        mock_client.uploads.delete.side_effect = Exception(
            "Failed to delete upload: 500 - boom"
        )
        with patch("mcp_tools.uploads.delete.get_client", return_value=mock_client):
            result = delete_upload("upload-1")
        assert "Failed to delete upload" in result
        assert "500" in result

    def test_unknown_server_response(self):
        mock_client = MagicMock()
        mock_client.uploads.delete.return_value = False
        with patch("mcp_tools.uploads.delete.get_client", return_value=mock_client):
            assert "unknown server response" in delete_upload("upload-1")
