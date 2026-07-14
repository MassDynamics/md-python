"""Tests for update_upload."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import update_upload


class TestUpdateUpload:
    def test_success(self):
        mock_client = MagicMock()
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1", name="new", description="desc")
        assert result == "Upload updated successfully. ID: upload-1"
        mock_client.uploads.update.assert_called_once_with(
            "upload-1", name="new", description="desc"
        )

    def test_partial_update_passes_none_through(self):
        mock_client = MagicMock()
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            update_upload("upload-1", name="new")
        # description=None so the resource layer omits it from the payload.
        mock_client.uploads.update.assert_called_once_with(
            "upload-1", name="new", description=None
        )

    def test_empty_description_is_forwarded_not_treated_as_missing(self):
        mock_client = MagicMock()
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1", description="")
        assert result.startswith("Upload updated successfully")
        mock_client.uploads.update.assert_called_once_with(
            "upload-1", name=None, description=""
        )

    def test_no_fields_returns_error_without_calling_server(self):
        mock_client = MagicMock()
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1")
        assert result == "Error: provide at least one of name or description"
        mock_client.uploads.update.assert_not_called()

    def test_422_returns_friendly_name_taken_message(self):
        mock_client = MagicMock()
        mock_client.uploads.update.side_effect = Exception(
            "Failed to update upload: 422 - Name has already been taken"
        )
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1", name="taken")
        assert result.startswith("Error: ")
        assert "unique within the organisation" in result
        assert "unchanged" in result

    def test_generic_error_uses_error_sentinel(self):
        mock_client = MagicMock()
        mock_client.uploads.update.side_effect = Exception(
            "Failed to update upload: 500 - boom"
        )
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1", name="new")
        assert result.startswith("Error: ")
        assert "500" in result

    def test_unknown_server_response(self):
        mock_client = MagicMock()
        mock_client.uploads.update.return_value = None
        with patch("mcp_tools.uploads.update.get_client", return_value=mock_client):
            result = update_upload("upload-1", name="new")
        assert result.startswith("Error: ")
        assert "unknown server response" in result
