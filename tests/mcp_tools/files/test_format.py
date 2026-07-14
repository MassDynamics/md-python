"""Tests for plan_wide_to_md_format and get_md_format_spec."""

import json

from mcp_tools.files import (
    get_md_format_spec,
    plan_wide_to_md_format,
    validate_md_format_ids,
)

from .conftest import write_tsv


class TestValidateMdFormatIds:
    def test_uniprot_accessions_pass(self, cleanup):
        path = write_tsv(
            [
                [
                    "ProteinGroup",
                    "GeneNames",
                    "SampleName",
                    "ProteinIntensity",
                    "Imputed",
                ],
                ["P84085", "ARF5", "s1", "25.1", "0"],
                ["P13569", "CFTR", "s1", "22.0", "0"],
                ["Q02790;P11474", "FKBP4", "s1", "20.5", "0"],
            ]
        )
        result = validate_md_format_ids(path)
        assert result.startswith("OK")

    def test_ensembl_ids_warn(self, cleanup):
        path = write_tsv(
            [
                [
                    "ProteinGroup",
                    "GeneNames",
                    "SampleName",
                    "ProteinIntensity",
                    "Imputed",
                ],
                ["ENSP00000000233.5", "ARF5", "s1", "25.1", "0"],
                ["ENSP00000000412.3", "M6PR", "s1", "22.0", "0"],
            ]
        )
        result = validate_md_format_ids(path)
        assert result.startswith("WARNING")
        assert "Ensembl" in result
        assert "UniProt" in result

    def test_missing_proteingroup_errors(self, cleanup):
        path = write_tsv(
            [
                ["GeneId", "SampleName", "GeneExpression"],
                ["ENSG00000123", "s1", "5.2"],
            ]
        )
        result = validate_md_format_ids(path)
        assert result.startswith("Error")


class TestPlanWideToMdFormat:
    def test_diann_tabular_auto_detected(self, cleanup):
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
        result = json.loads(plan_wide_to_md_format(path, source_hint="diann_tabular"))
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

    def test_metabolite_target_produces_metabolite_spec(self, cleanup):
        path = write_tsv(
            [
                ["MetaboliteId", "S1", "S2"],
                ["HMDB0000001", "100", "200"],
            ]
        )
        cleanup.append(path)
        result = json.loads(
            plan_wide_to_md_format(
                path,
                target="md_format_metabolite",
                annotation_columns=["MetaboliteId"],
            )
        )
        assert "MetaboliteIntensity" in result["md_format_spec"]
        assert "MetaboliteId" in result["conversion_script"]
        assert "Imputed" in result["conversion_script"]

    def test_notes_mention_full_matrix_long_format(self, cleanup):
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
        assert "full matrix" in combined
        assert "long format" in combined

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

    def test_transpose_inserts_transpose_block_in_script(self, cleanup):
        path = write_tsv(
            [
                ["SampleName", "P12345", "P67890"],
                ["Sample_A", "1e7", "2e7"],
                ["Sample_B", "3e7", "4e7"],
            ]
        )
        cleanup.append(path)
        result = json.loads(plan_wide_to_md_format(path, transpose=True))
        assert "conversion_script" in result
        assert ".T" in result["conversion_script"]
        assert "transpose" in " ".join(result["notes"]).lower()

    def test_transpose_auto_detected_when_first_col_is_samplename(self, cleanup):
        path = write_tsv(
            [
                ["SampleName", "ProteinA", "ProteinB"],
                ["Sample_A", "100", "200"],
            ]
        )
        cleanup.append(path)
        result = json.loads(plan_wide_to_md_format(path))
        assert ".T" in result["conversion_script"]
        assert "auto-detected" in " ".join(result["notes"])


class TestGetMdFormatSpec:
    def test_protein_spec_has_required_columns(self):
        result = json.loads(get_md_format_spec("protein"))
        spec = result["spec"]
        for col in (
            "ProteinGroupId",
            "ProteinGroup",
            "GeneNames",
            "SampleName",
            "ProteinIntensity",
            "Imputed",
        ):
            assert col in spec
        assert result["upload_source"] == "md_format"
        assert "conversion_template" in result

    def test_gene_spec(self):
        result = json.loads(get_md_format_spec("gene"))
        spec = result["spec"]
        for col in ("GeneId", "SampleName", "GeneExpression"):
            assert col in spec
        assert result["upload_source"] == "md_format_gene"

    def test_metabolite_spec(self):
        result = json.loads(get_md_format_spec("metabolite"))
        spec = result["spec"]
        for col in ("MetaboliteId", "SampleName", "MetaboliteIntensity", "Imputed"):
            assert col in spec
        assert result["upload_source"] == "md_format_metabolite"

    def test_metabolite_spec_notes_mention_full_matrix(self):
        result = json.loads(get_md_format_spec("metabolite"))
        combined = " ".join(result["notes"]).lower()
        assert "full matrix" in combined
        assert "long format" in combined

    def test_metabolite_spec_documents_metadata_passthrough(self):
        result = json.loads(get_md_format_spec("metabolite"))
        # The spec lists the pass-through metadata column entry...
        assert any("metadata" in k.lower() for k in result["spec"])
        # ...and the notes explain the per-metabolite uniqueness rule.
        combined = " ".join(result["notes"]).lower()
        assert "pass-through" in combined
        assert "one value per metaboliteid" in combined or "constant" in combined

    def test_metabolite_spec_includes_example(self):
        result = json.loads(get_md_format_spec("metabolite"))
        example = result["example"]
        # Header carries the four required columns plus pass-through metadata.
        header = example.splitlines()[0]
        for col in ("MetaboliteId", "MetaboliteIntensity", "SampleName", "Imputed"):
            assert col in header
        # Pass-through metadata column carrying the human-readable name.
        assert "MetaboliteName" in header
        # A missing measurement is shown as intensity 0.0 with Imputed=1.
        assert "\t0.0\t1" in example
        # Other entity types do not carry an example field.
        assert "example" not in json.loads(get_md_format_spec("protein"))

    def test_peptide_spec(self):
        result = json.loads(get_md_format_spec("peptide"))
        spec = result["spec"]
        for col in (
            "ModifiedSequence",
            "StrippedSequence",
            "Unique",
            "ProteinGroup",
            "ProteinGroupId",
            "SampleName",
            "PeptideIntensity",
            "Imputed",
        ):
            assert col in spec

    def test_peptide_spec_documents_dual_file_and_unique(self):
        result = json.loads(get_md_format_spec("peptide"))
        notes = " ".join(result["notes"]).lower()
        # dual-file requirement and the cross-table id rule must be surfaced
        assert "dual-file" in notes or "protein-level" in notes
        assert "unique" in notes
        assert "identical" in notes or "do not factorize" in notes
        # the conversion template must emit Unique and derive ids from the
        # protein companion (not an independent factorize)
        template = result["conversion_template"]
        assert "Unique" in template
        assert "pg_to_id" in template

    def test_peptide_notes_cover_inline_unimod_and_unlocalised(self):
        result = json.loads(get_md_format_spec("peptide"))
        notes = " ".join(result["notes"]).lower()
        # ModifiedSequence must be inline UniMod, not a tool's native annotation
        assert "inline unimod" in notes
        assert "proteome discoverer" in notes
        # unlocalised-mod disclaimer must be put to the user (drop vs first-available)
        assert "unlocalised" in notes or "unlocalized" in notes
        assert "first candidate" in notes or "drop the unlocalised" in notes
        # must NOT have re-introduced the reverted {p} probability suffix
        assert "{0.01}" not in notes and "{p}" not in notes

    def test_default_is_protein(self):
        default = json.loads(get_md_format_spec())
        protein = json.loads(get_md_format_spec("protein"))
        assert default["spec"] == protein["spec"]

    def test_notes_mention_samplename_alignment(self):
        result = json.loads(get_md_format_spec())
        combined = " ".join(result["notes"]).lower()
        assert "samplename" in combined or "sample_name" in combined
