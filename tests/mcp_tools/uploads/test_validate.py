"""Tests for validate_upload_inputs."""

from mcp_tools.uploads import validate_upload_inputs

from .conftest import DESIGN, METADATA


class TestValidateUploadInputs:
    def test_valid_inputs(self):
        result = validate_upload_inputs(DESIGN, METADATA)
        assert result.startswith("OK")
        assert "2 samples" in result

    def test_synonym_columns_accepted(self):
        design = [
            ["file", "sample", "group"],
            ["file1.tsv", "s1", "ctrl"],
            ["file2.tsv", "s2", "treated"],
        ]
        assert validate_upload_inputs(design, METADATA).startswith("OK")

    def test_missing_sample_in_metadata(self):
        metadata = [["sample_name", "dose"], ["s1", "0"]]  # s2 absent
        result = validate_upload_inputs(DESIGN, metadata)
        assert "Validation failed" in result
        assert "s2" in result

    def test_extra_sample_in_metadata(self):
        metadata = [["sample_name", "dose"], ["s1", "0"], ["s2", "10"], ["s3", "20"]]
        result = validate_upload_inputs(DESIGN, metadata)
        assert "Validation failed" in result
        assert "s3" in result

    def test_missing_required_column_in_design(self):
        bad_design = [["filename", "sample_name"], ["file1.tsv", "s1"]]
        result = validate_upload_inputs(bad_design, METADATA)
        assert "missing required column" in result

    def test_missing_sample_name_column_in_metadata(self):
        bad_metadata = [["name", "dose"], ["s1", "0"]]
        result = validate_upload_inputs(DESIGN, bad_metadata)
        assert "sample_name" in result

    def test_duplicate_sample_in_design(self):
        dupe = [
            ["filename", "sample_name", "condition"],
            ["file1.tsv", "s1", "ctrl"],
            ["file2.tsv", "s1", "ctrl"],
        ]
        result = validate_upload_inputs(dupe, METADATA)
        assert "Duplicate" in result
        assert "s1" in result

    def test_empty_design_returns_error(self):
        result = validate_upload_inputs([], METADATA)
        assert "Error" in result
        assert "experiment_design" in result

    def test_empty_metadata_returns_error(self):
        result = validate_upload_inputs(DESIGN, [])
        assert "Error" in result
        assert "sample_metadata" in result
