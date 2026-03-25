"""Tests for load_metadata_from_csv."""

import json

from mcp_tools.files import load_metadata_from_csv

from .conftest import write_csv, write_tsv


class TestLoadMetadataFromCsv:
    def test_combined_file_returns_both_tables(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition", "dose"],
                ["s1.raw", "S1", "ctrl", "0"],
                ["s2.raw", "S2", "treated", "10"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is not None
        assert result["sample_metadata"] is not None
        assert result["sample_count"] == 2
        assert result["experiment_design"][0] == [
            "filename",
            "sample_name",
            "condition",
        ]
        assert "filename" not in result["sample_metadata"][0]

    def test_combined_file_deduplicates_sample_metadata(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition", "dose"],
                ["s1_rep1.raw", "S1", "ctrl", "0"],
                ["s1_rep2.raw", "S1", "ctrl", "0"],
                ["s2.raw", "S2", "treated", "10"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert len(result["experiment_design"]) == 4  # header + 3 rows
        assert len(result["sample_metadata"]) == 3  # header + 2 unique samples

    def test_design_only_file(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
                ["s2.raw", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is not None
        assert result["sample_metadata"] is not None

    def test_sample_metadata_only_file(self, cleanup):
        path = write_csv(
            [
                ["sample_name", "dose", "batch"],
                ["S1", "0", "A"],
                ["S2", "10", "A"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is None
        assert result["sample_metadata"] is not None
        assert "no filename" in " ".join(result["notes"]).lower()

    def test_synonym_columns_normalised(self, cleanup):
        path = write_csv(
            [
                ["file", "sample", "group", "dose"],
                ["s1.raw", "S1", "ctrl", "0"],
                ["s2.raw", "S2", "treated", "10"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"][0] == [
            "filename",
            "sample_name",
            "condition",
        ]

    def test_tsv_file(self, cleanup):
        path = write_tsv(
            [
                ["filename", "sample_name", "condition", "dose"],
                ["s1.raw", "S1", "ctrl", "0"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["sample_count"] == 1

    def test_sample_name_is_first_column_in_metadata(self, cleanup):
        path = write_csv(
            [
                ["filename", "dose", "sample_name", "condition"],
                ["s1.raw", "0", "S1", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["sample_metadata"][0][0].lower() in ("sample_name", "sample")

    def test_notes_always_include_validate_reminder(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert "validate_upload_inputs" in " ".join(result["notes"]).lower()

    def test_warns_on_empty_condition_value(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", ""],
                ["s2.raw", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert "empty condition" in " ".join(result["notes"])

    def test_missing_sample_name_column_returns_error(self, cleanup):
        path = write_csv(
            [
                ["filename", "group"],
                ["s1.raw", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert "error" in result
        assert "sample_name" in result["error"]

    def test_file_not_found(self):
        result = json.loads(load_metadata_from_csv("/no/such/file.csv"))
        assert "error" in result

    def test_rejects_entity_data_file(self, cleanup):
        path = write_tsv(
            [
                ["File.Name", "Protein.Group", "PG.MaxLFQ"],
                ["/data/s1.raw", "P12345", "1e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert "error" in result
        assert "STOP" in result["error"]


class TestLoadMetadataEdgeCases:
    def test_ragged_row_shorter_than_header(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["f1", "s1"],  # missing condition
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is not None
        assert result["experiment_design"][1][2] == ""

    def test_sample_name_whitespace_stripped(self, cleanup):
        path = write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["f1", "  s1  ", "ctrl"],
                ["f2", "  s2  ", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"][1][1] == "s1"
        assert result["experiment_design"][2][1] == "s2"
