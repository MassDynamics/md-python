"""Tests for get_upload and update_sample_metadata."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import get_upload, update_sample_metadata

from .conftest import METADATA


class TestGetUpload:
    def test_by_id(self):
        mock_upload = MagicMock()
        mock_upload.__str__ = lambda self: "Upload: test"
        mock_client = MagicMock()
        mock_client.uploads.get_by_id.return_value = mock_upload

        with patch("mcp_tools.uploads.get.get_client", return_value=mock_client):
            result = get_upload(upload_id="abc-123")

        mock_client.uploads.get_by_id.assert_called_once_with("abc-123")
        assert "Upload: test" in result

    def test_by_name(self):
        mock_upload = MagicMock()
        mock_upload.__str__ = lambda self: "Upload: my-upload"
        mock_client = MagicMock()
        mock_client.uploads.get_by_name.return_value = mock_upload

        with patch("mcp_tools.uploads.get.get_client", return_value=mock_client):
            result = get_upload(name="my-upload")

        mock_client.uploads.get_by_name.assert_called_once_with("my-upload")
        assert "Upload: my-upload" in result

    def test_not_found(self):
        mock_client = MagicMock()
        mock_client.uploads.get_by_id.return_value = None

        with patch("mcp_tools.uploads.get.get_client", return_value=mock_client):
            result = get_upload(upload_id="missing")

        assert "not found" in result.lower()

    def test_no_args_returns_error(self):
        assert "Error" in get_upload()


class TestUpdateSampleMetadata:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.uploads.update_sample_metadata.return_value = True

        with patch("mcp_tools.uploads.get.get_client", return_value=mock_client):
            result = update_sample_metadata("upload-123", METADATA)

        assert "successfully" in result

    def test_failure(self):
        mock_client = MagicMock()
        mock_client.uploads.update_sample_metadata.return_value = False

        with patch("mcp_tools.uploads.get.get_client", return_value=mock_client):
            result = update_sample_metadata("upload-123", METADATA)

        assert "Failed" in result
