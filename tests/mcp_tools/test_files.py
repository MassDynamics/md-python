import csv
import json
import os
import tempfile

import pytest

from mcp_tools.files import (
    load_metadata_from_csv,
    plan_wide_to_md_format,
    read_csv_preview,
)


def _write_csv(rows, suffix=".csv"):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False, newline="", encoding="utf-8"
    )
    writer = csv.writer(f)
    writer.writerows(rows)
    f.close()
    return f.name


def _write_tsv(rows):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".tsv", delete=False, newline="", encoding="utf-8"
    )
    writer = csv.writer(f, delimiter="\t")
    writer.writerows(rows)
    f.close()
    return f.name


@pytest.fixture(autouse=True)
def cleanup(tmp_path):
    """Files created in tests are cleaned up after each test."""
    created = []
    yield created
    for p in created:
        try:
            os.unlink(p)
        except OSError:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# read_csv_preview
# ──────────────────────────────────────────────────────────────────────────────


class TestReadCsvPreview:
    def test_basic_metadata_csv(self, cleanup):
        path = _write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
                ["s2.raw", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "filename" in result
        assert "sample_name" in result
        assert "S1" in result

    def test_tsv_auto_delimiter(self, cleanup):
        path = _write_tsv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "tab" in result
        assert "filename" in result

    def test_file_not_found(self):
        result = read_csv_preview("/nonexistent/path/file.csv")
        assert "Error" in result

    def test_max_rows_respected(self, cleanup):
        rows = [["filename", "sample_name", "condition"]] + [
            [f"s{i}.raw", f"S{i}", "ctrl"] for i in range(10)
        ]
        path = _write_csv(rows)
        cleanup.append(path)
        result = read_csv_preview(path, max_rows=3)
        assert "[3]" in result
        assert "[4]" not in result

    # Entity data rejection tests (one per format)

    def test_rejects_diann_report_file_name_col(self, cleanup):
        path = _write_tsv(
            [
                ["File.Name", "Protein.Group", "Genes", "PG.MaxLFQ"],
                ["/data/s1.raw", "P12345", "EGFR", "1234567.0"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result
        assert "DIA-NN" in result

    def test_rejects_maxquant_protein_groups(self, cleanup):
        path = _write_tsv(
            [
                [
                    "Majority protein IDs",
                    "Gene names",
                    "LFQ intensity S1",
                    "LFQ intensity S2",
                ],
                ["P12345;P67890", "EGFR", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_spectronaut_report(self, cleanup):
        path = _write_tsv(
            [
                ["R.FileName", "PG.GroupLabel", "PG.Quantity"],
                ["s1.raw", "P12345", "1234567"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result
        assert "Spectronaut" in result

    def test_rejects_md_format_protein_table(self, cleanup):
        path = _write_tsv(
            [
                [
                    "ProteinGroupId",
                    "GeneNames",
                    "SampleName",
                    "ProteinIntensity",
                    "Imputed",
                ],
                ["1", "EGFR", "S1", "1e7", "0"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_md_format_gene_table(self, cleanup):
        path = _write_csv(
            [
                ["GeneId", "SampleName", "GeneExpression"],
                ["ENSG001", "S1", "1234"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_msfragger_combined_protein(self, cleanup):
        path = _write_tsv(
            [
                ["Protein ID", "Protein", "S1 Intensity", "S2 Intensity"],
                ["sp|P12345|EGFR", "EGFR_HUMAN", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_diann_matrix(self, cleanup):
        path = _write_tsv(
            [
                ["Protein.Group", "Protein.Ids", "sample1.raw", "sample2.raw"],
                ["P12345", "P12345;P67890", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result


# ──────────────────────────────────────────────────────────────────────────────
# load_metadata_from_csv
# ──────────────────────────────────────────────────────────────────────────────


class TestLoadMetadataFromCsv:
    def test_combined_file_returns_both(self, cleanup):
        path = _write_csv(
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
        # experiment_design only keeps 3 columns
        assert result["experiment_design"][0] == [
            "filename",
            "sample_name",
            "condition",
        ]
        # sample_metadata has sample_name + dose (no filename)
        sm_header = result["sample_metadata"][0]
        assert "sample_name" in sm_header or "sample_name" in [
            h.lower() for h in sm_header
        ]
        assert "filename" not in sm_header

    def test_combined_file_deduplicates_sample_metadata(self, cleanup):
        # Each sample_name appears once in sample_metadata even if multiple design rows
        path = _write_csv(
            [
                ["filename", "sample_name", "condition", "dose"],
                ["s1_rep1.raw", "S1", "ctrl", "0"],
                ["s1_rep2.raw", "S1", "ctrl", "0"],
                ["s2.raw", "S2", "treated", "10"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        # design has 3 data rows
        assert len(result["experiment_design"]) == 4  # header + 3
        # metadata has 2 unique samples
        assert len(result["sample_metadata"]) == 3  # header + 2

    def test_design_only_file(self, cleanup):
        path = _write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
                ["s2.raw", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is not None
        # sample_metadata only has sample_name column — still returned
        assert result["sample_metadata"] is not None

    def test_sample_metadata_only_file(self, cleanup):
        path = _write_csv(
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

    def test_synonym_columns_accepted(self, cleanup):
        path = _write_csv(
            [
                ["file", "sample", "group", "dose"],
                ["s1.raw", "S1", "ctrl", "0"],
                ["s2.raw", "S2", "treated", "10"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["experiment_design"] is not None
        assert result["experiment_design"][0] == [
            "filename",
            "sample_name",
            "condition",
        ]

    def test_tsv_file(self, cleanup):
        path = _write_tsv(
            [
                ["filename", "sample_name", "condition", "dose"],
                ["s1.raw", "S1", "ctrl", "0"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert result["sample_count"] == 1

    def test_missing_sample_name_column(self, cleanup):
        path = _write_csv(
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
        path = _write_tsv(
            [
                ["File.Name", "Protein.Group", "PG.MaxLFQ"],
                ["/data/s1.raw", "P12345", "1e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        assert "error" in result
        assert "STOP" in result["error"]

    def test_sample_name_is_first_in_metadata(self, cleanup):
        """sample_name must be the first column in sample_metadata."""
        path = _write_csv(
            [
                ["filename", "dose", "sample_name", "condition"],
                ["s1.raw", "0", "S1", "ctrl"],
                ["s2.raw", "10", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        sm_header = result["sample_metadata"][0]
        assert sm_header[0].lower() in ("sample_name", "sample")

    def test_notes_include_validate_reminder(self, cleanup):
        path = _write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        combined_notes = " ".join(result["notes"]).lower()
        assert "validate_upload_inputs" in combined_notes

    def test_warns_on_empty_condition(self, cleanup):
        path = _write_csv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", ""],
                ["s2.raw", "S2", "treated"],
            ]
        )
        cleanup.append(path)
        result = json.loads(load_metadata_from_csv(path))
        combined_notes = " ".join(result["notes"])
        assert "empty condition" in combined_notes


# ──────────────────────────────────────────────────────────────────────────────
# plan_wide_to_md_format
# ──────────────────────────────────────────────────────────────────────────────


class TestPlanWideToMdFormat:
    def test_diann_matrix_auto_detect(self, cleanup):
        path = _write_tsv(
            [
                [
                    "Protein.Group",
                    "Protein.Ids",
                    "Genes",
                    "First.Protein.Description",
                    "/data/s1.raw",
                    "/data/s2.raw",
                ],
                [
                    "P12345",
                    "P12345;P67890",
                    "EGFR",
                    "Epidermal growth factor receptor",
                    "1e7",
                    "2e7",
                ],
            ]
        )
        cleanup.append(path)
        result = json.loads(plan_wide_to_md_format(path, source_hint="diann_matrix"))
        assert "conversion_script" in result
        assert "Protein.Group" in result["detected_annotation_cols"]
        assert "/data/s1.raw" in result["detected_sample_cols"]
        assert "pd.read_csv" in result["conversion_script"]
        assert "melt" in result["conversion_script"]

    def test_explicit_annotation_columns(self, cleanup):
        path = _write_tsv(
            [
                ["GeneSymbol", "Description", "S1", "S2", "S3"],
                ["EGFR", "EGF receptor", "1e7", "2e7", "3e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path,
                annotation_columns=["GeneSymbol", "Description"],
            )
        )
        assert "GeneSymbol" in result["detected_annotation_cols"]
        assert "S1" in result["detected_sample_cols"]
        assert result["sample_col_count"] == 3

    def test_md_format_gene_target(self, cleanup):
        path = _write_tsv(
            [
                ["GeneId", "S1", "S2"],
                ["ENSG001", "100", "200"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path,
                target="md_format_gene",
                annotation_columns=["GeneId"],
            )
        )
        assert "GeneExpression" in result["md_format_spec"]
        assert "GeneId" in result["conversion_script"]

    def test_md_format_spec_has_required_columns(self, cleanup):
        path = _write_tsv(
            [
                ["Protein.Group", "Genes", "S1", "S2"],
                ["P12345", "EGFR", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(path, annotation_columns=["Protein.Group", "Genes"])
        )
        spec = result["md_format_spec"]
        for col in (
            "ProteinGroupId",
            "ProteinGroup",
            "GeneNames",
            "SampleName",
            "ProteinIntensity",
            "Imputed",
        ):
            assert col in spec

    def test_file_not_found(self):
        result = json.loads(plan_wide_to_md_format("/no/such/file.tsv"))
        assert "error" in result

    def test_all_annotation_columns_returns_error(self, cleanup):
        path = _write_tsv(
            [
                ["Protein.Group", "Genes"],
                ["P12345", "EGFR"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path,
                annotation_columns=["Protein.Group", "Genes"],
            )
        )
        assert "error" in result

    def test_notes_mention_sample_name_alignment(self, cleanup):
        path = _write_tsv(
            [
                ["Protein.Group", "S1", "S2"],
                ["P12345", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(path, annotation_columns=["Protein.Group"])
        )
        combined_notes = " ".join(result["notes"]).lower()
        assert "sample_name" in combined_notes or "samplename" in combined_notes
