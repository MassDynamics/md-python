"""Tests for plan_wide_to_md_format."""

import json

from mcp_tools.files import plan_wide_to_md_format

from .conftest import write_tsv


class TestPlanWideToMdFormat:
    def test_diann_matrix_auto_detected(self, cleanup):
        path = write_tsv(
            [
                [
                    "Protein.Group",
                    "Protein.Ids",
                    "Genes",
                    "First.Protein.Description",
                    "/data/s1.raw",
                    "/data/s2.raw",
                ],
                ["P12345", "P12345;P67890", "EGFR", "EGF receptor", "1e7", "2e7"],
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
        path = write_tsv(
            [
                ["GeneSymbol", "Description", "S1", "S2", "S3"],
                ["EGFR", "EGF receptor", "1e7", "2e7", "3e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path, annotation_columns=["GeneSymbol", "Description"]
            )
        )
        assert "GeneSymbol" in result["detected_annotation_cols"]
        assert "S1" in result["detected_sample_cols"]
        assert result["sample_col_count"] == 3

    def test_gene_target_produces_gene_expression_spec(self, cleanup):
        path = write_tsv(
            [
                ["GeneId", "S1", "S2"],
                ["ENSG001", "100", "200"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path, target="md_format_gene", annotation_columns=["GeneId"]
            )
        )
        assert "GeneExpression" in result["md_format_spec"]
        assert "GeneId" in result["conversion_script"]

    def test_md_format_spec_contains_required_columns(self, cleanup):
        path = write_tsv(
            [
                ["Protein.Group", "Genes", "S1", "S2"],
                ["P12345", "EGFR", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(path, annotation_columns=["Protein.Group", "Genes"])
        )
        for col in (
            "ProteinGroupId",
            "ProteinGroup",
            "GeneNames",
            "SampleName",
            "ProteinIntensity",
            "Imputed",
        ):
            assert col in result["md_format_spec"]

    def test_notes_mention_sample_name_alignment(self, cleanup):
        path = write_tsv(
            [
                ["Protein.Group", "S1", "S2"],
                ["P12345", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(path, annotation_columns=["Protein.Group"])
        )
        combined = " ".join(result["notes"]).lower()
        assert "sample_name" in combined or "samplename" in combined

    def test_file_not_found(self):
        result = json.loads(plan_wide_to_md_format("/no/such/file.tsv"))
        assert "error" in result

    def test_all_annotation_columns_returns_error(self, cleanup):
        path = write_tsv(
            [
                ["Protein.Group", "Genes"],
                ["P12345", "EGFR"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(path, annotation_columns=["Protein.Group", "Genes"])
        )
        assert "error" in result
