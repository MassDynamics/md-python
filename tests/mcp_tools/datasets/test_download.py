"""Tests for download_dataset_table."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import download_dataset_table


class TestDownloadDatasetTable:
    def test_returns_url_when_no_output_path(self):
        mock_client = MagicMock()
        mock_client.datasets.download_table_url.return_value = (
            "https://s3.example.com/presigned"
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(
                download_dataset_table("ds-1", "protein_intensity", format="csv")
            )

        mock_client.datasets.download_table_url.assert_called_once_with(
            "ds-1", "protein_intensity", format="csv"
        )
        assert result["download_url"] == "https://s3.example.com/presigned"
        assert result["dataset_id"] == "ds-1"
        assert result["table_name"] == "protein_intensity"
        assert result["format"] == "csv"

    def test_writes_file_when_output_path_given(self, tmp_path):
        mock_client = MagicMock()
        mock_client.datasets.download_table_url.return_value = (
            "https://s3.example.com/presigned"
        )

        fake_response = MagicMock()
        fake_response.__enter__.return_value = fake_response
        fake_response.__exit__.return_value = False
        fake_response.raise_for_status.return_value = None
        fake_response.iter_content.return_value = [b"abc", b"defg"]

        out = tmp_path / "protein.csv"

        with (
            patch("mcp_tools.datasets.download.get_client", return_value=mock_client),
            patch(
                "mcp_tools.datasets.download.requests.get", return_value=fake_response
            ) as mock_get,
        ):
            result = json.loads(
                download_dataset_table(
                    "ds-1",
                    "protein_intensity",
                    format="csv",
                    output_path=str(out),
                )
            )

        mock_get.assert_called_once()
        assert result["path"] == str(out)
        assert result["bytes"] == 7
        assert out.read_bytes() == b"abcdefg"

    def test_rejects_invalid_format(self):
        result = json.loads(
            download_dataset_table("ds-1", "protein_intensity", format="xml")
        )
        assert "error" in result
        assert "Invalid format" in result["error"]

    def test_propagates_client_error(self):
        mock_client = MagicMock()
        mock_client.datasets.download_table_url.side_effect = Exception(
            "Failed to get download URL: 404"
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "protein_intensity"))
        assert "error" in result
        assert "404" in result["error"]

    def test_propagates_stream_error(self, tmp_path):
        mock_client = MagicMock()
        mock_client.datasets.download_table_url.return_value = (
            "https://s3.example.com/presigned"
        )
        with (
            patch("mcp_tools.datasets.download.get_client", return_value=mock_client),
            patch(
                "mcp_tools.datasets.download.requests.get",
                side_effect=RuntimeError("network blew up"),
            ),
        ):
            result = json.loads(
                download_dataset_table(
                    "ds-1",
                    "protein_intensity",
                    output_path=str(tmp_path / "out.csv"),
                )
            )
        assert "error" in result
        assert "network blew up" in result["error"]
