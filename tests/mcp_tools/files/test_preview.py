"""Tests for read_csv_preview."""

from mcp_tools.files import read_csv_preview

from .conftest import write_csv, write_tsv


class TestReadCsvPreview:
    def test_basic_metadata_csv(self, cleanup):
        path = write_csv(
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

    def test_tsv_auto_detects_tab_delimiter(self, cleanup):
        path = write_tsv(
            [
                ["filename", "sample_name", "condition"],
                ["s1.raw", "S1", "ctrl"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "tab" in result
        assert "filename" in result

    def test_max_rows_limits_output(self, cleanup):
        rows = [["filename", "sample_name", "condition"]] + [
            [f"s{i}.raw", f"S{i}", "ctrl"] for i in range(10)
        ]
        path = write_csv(rows)
        cleanup.append(path)
        result = read_csv_preview(path, max_rows=3)
        assert "[3]" in result
        assert "[4]" not in result

    def test_file_not_found(self):
        result = read_csv_preview("/nonexistent/path/file.csv")
        assert "Error" in result


class TestReadCsvPreviewEntityDataRejection:
    """read_csv_preview must stop immediately on entity-data files."""

    def test_rejects_diann_report(self, cleanup):
        path = write_tsv(
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
        path = write_tsv(
            [
                ["Majority protein IDs", "Gene names", "LFQ intensity S1"],
                ["P12345;P67890", "EGFR", "1e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_spectronaut_report(self, cleanup):
        path = write_tsv(
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
        path = write_tsv(
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
        path = write_csv(
            [
                ["GeneId", "SampleName", "GeneExpression"],
                ["ENSG001", "S1", "1234"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_msfragger_combined_protein(self, cleanup):
        path = write_tsv(
            [
                ["Protein ID", "Protein", "S1 Intensity"],
                ["sp|P12345|EGFR", "EGFR_HUMAN", "1e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result

    def test_rejects_diann_matrix(self, cleanup):
        path = write_tsv(
            [
                ["Protein.Group", "Protein.Ids", "sample1.raw", "sample2.raw"],
                ["P12345", "P12345;P67890", "1e7", "2e7"],
            ]
        )
        cleanup.append(path)
        result = read_csv_preview(path)
        assert "STOP" in result
