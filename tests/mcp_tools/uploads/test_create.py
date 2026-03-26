"""Tests for create_upload and create_upload_from_csv."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import create_upload, create_upload_from_csv

from .conftest import DESIGN, METADATA


class TestCreateUpload:
    def test_s3_upload(self):
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-001"

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload(
                name="Test Upload",
                source="diann_tabular",
                experiment_design=DESIGN,
                sample_metadata=METADATA,
                s3_bucket="my-bucket",
                s3_prefix="data/",
                filenames=["report.tsv"],
            )

        assert "upload-id-001" in result
        mock_client.uploads.create.assert_called_once()

    def test_local_files_explicit(self):
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-002"

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload(
                name="Test Upload",
                source="diann_tabular",
                experiment_design=DESIGN,
                sample_metadata=METADATA,
                file_location="/data/files",
                filenames=["report.tsv"],
            )

        assert "upload-id-002" in result

    def test_auto_discovers_filenames_from_directory(self, tmp_path):
        (tmp_path / "report.tsv").write_text("data")
        (tmp_path / "metadata.csv").write_text("data")
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-003"

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload(
                name="Test",
                source="diann_tabular",
                experiment_design=DESIGN,
                sample_metadata=METADATA,
                file_location=str(tmp_path),
            )

        assert "upload-id-003" in result
        call_args = mock_client.uploads.create.call_args[0][0]
        assert sorted(call_args.filenames) == sorted(["metadata.csv", "report.tsv"])


class TestCreateUploadFromCsv:
    def test_returns_upload_id(self, tmp_path):
        csv = tmp_path / "metadata.csv"
        csv.write_text(
            "filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n"
        )
        (tmp_path / "file1.tsv").write_text("data")
        (tmp_path / "file2.tsv").write_text("data")
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-from-csv"

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload_from_csv(
                name="CSV Upload",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location=str(tmp_path),
            )

        assert "upload-id-from-csv" in result
        _, kwargs = mock_client.uploads.create.call_args
        assert kwargs.get("background") is True

    def test_bad_csv_returns_error(self, tmp_path):
        csv = tmp_path / "bad.csv"
        csv.write_text("sample_name,dose\ns1,0\ns2,10\n")  # missing filename/condition

        with patch("mcp_tools.uploads.create.get_client", return_value=MagicMock()):
            result = create_upload_from_csv(
                name="Bad",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location=str(tmp_path),
            )

        assert "Error" in result

    def test_missing_file_location_returns_error(self, tmp_path):
        csv = tmp_path / "metadata.csv"
        csv.write_text("filename,sample_name,condition\nfile1,s1,ctrl\n")

        with patch("mcp_tools.uploads.create.get_client", return_value=MagicMock()):
            result = create_upload_from_csv(
                name="Test",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location="/nonexistent/path",
            )

        assert "Error" in result
        assert "file_location" in result

    def test_large_files_use_sequential_executor(self, tmp_path):
        from mcp_tools.uploads._executor import _large_upload_executor

        csv = tmp_path / "metadata.csv"
        csv.write_text(
            "filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n"
        )
        (tmp_path / "file1.tsv").write_text("data")
        (tmp_path / "file2.tsv").write_text("data")
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-large"

        large_size = 60 * 1024 * 1024  # 60 MB × 2 files = 120 MB > 100 MB threshold
        with (
            patch("mcp_tools.uploads.create.get_client", return_value=mock_client),
            patch("mcp_tools.uploads.create.os.path.getsize", return_value=large_size),
        ):
            result = create_upload_from_csv(
                name="Large Upload",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location=str(tmp_path),
            )

        assert "queued" in result
        _, kwargs = mock_client.uploads.create.call_args
        assert kwargs.get("executor") is _large_upload_executor

    def test_small_files_use_no_executor(self, tmp_path):
        csv = tmp_path / "metadata.csv"
        csv.write_text(
            "filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n"
        )
        (tmp_path / "file1.tsv").write_text("data")
        (tmp_path / "file2.tsv").write_text("data")
        mock_client = MagicMock()
        mock_client.uploads.create.return_value = "upload-id-small"

        small_size = 10 * 1024 * 1024  # 10 MB × 2 = 20 MB < 100 MB threshold
        with (
            patch("mcp_tools.uploads.create.get_client", return_value=mock_client),
            patch("mcp_tools.uploads.create.os.path.getsize", return_value=small_size),
        ):
            result = create_upload_from_csv(
                name="Small Upload",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location=str(tmp_path),
            )

        assert "queued" not in result
        _, kwargs = mock_client.uploads.create.call_args
        assert kwargs.get("executor") is None
