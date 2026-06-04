"""Tests for create_upload and create_upload_from_csv."""

from unittest.mock import MagicMock, patch

from mcp_tools.uploads import create_upload, create_upload_from_csv
from mcp_tools.uploads.create import _check_md_format_composition

from .conftest import DESIGN, METADATA

_PROTEIN_HEADER = (
    "ProteinGroupId\tProteinGroup\tGeneNames\tSampleName\tProteinIntensity\tImputed\n"
)
_PEPTIDE_HEADER = (
    "ModifiedSequence\tStrippedSequence\tUnique\tProteinGroup\tProteinGroupId\t"
    "GeneNames\tSampleName\tPeptideIntensity\tImputed\n"
)


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
        from mcp_tools.uploads._executor import _get_executor

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
        # Assert against the live singleton — not a captured binding — so
        # this test is robust to other tests that call _reset_executor().
        assert kwargs.get("executor") is _get_executor()

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


class TestMdFormatCompositionGuard:
    """A peptide-only md_format upload must be rejected before transfer
    (the code-level guard deferred by the get_md_format_spec peptide docs)."""

    def test_helper_rejects_peptide_only(self, tmp_path):
        (tmp_path / "peptide.tsv").write_text(_PEPTIDE_HEADER + "PEPT(UniMod:21)IDE\t")
        err = _check_md_format_composition("md_format", str(tmp_path), ["peptide.tsv"])
        assert err is not None
        assert "peptide-only" in err
        assert "protein" in err.lower()

    def test_helper_allows_protein_plus_peptide(self, tmp_path):
        (tmp_path / "protein.tsv").write_text(_PROTEIN_HEADER + "1\tP12345\t")
        (tmp_path / "peptide.tsv").write_text(_PEPTIDE_HEADER + "PEPT(UniMod:21)IDE\t")
        err = _check_md_format_composition(
            "md_format", str(tmp_path), ["protein.tsv", "peptide.tsv"]
        )
        assert err is None

    def test_helper_allows_protein_only(self, tmp_path):
        (tmp_path / "protein.tsv").write_text(_PROTEIN_HEADER + "1\tP12345\t")
        err = _check_md_format_composition("md_format", str(tmp_path), ["protein.tsv"])
        assert err is None

    def test_helper_skips_non_md_format_source(self, tmp_path):
        # A DIA-NN pr_matrix has peptide-ish columns but is NOT md_format —
        # the guard must not fire for it.
        (tmp_path / "peptide.tsv").write_text(_PEPTIDE_HEADER)
        err = _check_md_format_composition(
            "diann_tabular", str(tmp_path), ["peptide.tsv"]
        )
        assert err is None

    def test_helper_skips_s3_upload(self):
        # No local file_location → cannot read headers → not applicable.
        err = _check_md_format_composition("md_format", None, ["peptide.tsv"])
        assert err is None

    def test_create_upload_from_csv_rejects_peptide_only(self, tmp_path):
        csv = tmp_path / "metadata.csv"
        csv.write_text("filename,sample_name,condition\npeptide.tsv,s1,ctrl\n")
        (tmp_path / "peptide.tsv").write_text(_PEPTIDE_HEADER + "PEPT(UniMod:21)IDE\t")
        mock_client = MagicMock()

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload_from_csv(
                name="Peptide only",
                source="md_format",
                metadata_csv_path=str(csv),
                file_location=str(tmp_path),
                filenames=["peptide.tsv"],
            )

        assert "peptide-only" in result
        mock_client.uploads.create.assert_not_called()

    def test_create_upload_rejects_peptide_only(self, tmp_path):
        (tmp_path / "peptide.tsv").write_text(_PEPTIDE_HEADER + "PEPT(UniMod:21)IDE\t")
        mock_client = MagicMock()

        with patch("mcp_tools.uploads.create.get_client", return_value=mock_client):
            result = create_upload(
                name="Peptide only",
                source="md_format",
                experiment_design=DESIGN,
                sample_metadata=METADATA,
                file_location=str(tmp_path),
                filenames=["peptide.tsv"],
            )

        assert "peptide-only" in result
        mock_client.uploads.create.assert_not_called()
