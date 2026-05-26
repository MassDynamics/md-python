"""Tests for the de_method plumbing on pairwise comparison.

Ground truth: PairwiseParamsProperties in MDFlexiComparisons declares five
separate per-entity de_method fields. Only entity_type='gene' allows
edgeR / DESeq2; every other entity is hard-pinned to limma.
"""

from uuid import UUID

import pytest

from md_python.models.dataset_builders import PairwiseComparisonDataset
from md_python.models.metadata import SampleMetadata


def _sm():
    return SampleMetadata(
        data=[["sample_name", "condition"], ["s1", "a"], ["s2", "b"]]
    )


def _base(**overrides):
    kwargs = dict(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="PW",
        sample_metadata=_sm(),
        condition_column="condition",
        condition_comparisons=[["a", "b"]],
    )
    kwargs.update(overrides)
    return PairwiseComparisonDataset(**kwargs)


class TestDeMethodWireFormat:
    @pytest.mark.parametrize(
        "entity_type,key",
        [
            ("protein", "de_method_protein"),
            ("peptide", "de_method_peptide"),
            ("gene", "de_method_gene"),
            ("metabolite", "de_method_metabolite"),
            ("ptm", "de_method_ptm"),
        ],
    )
    def test_emits_entity_keyed_de_method_field(self, entity_type, key):
        ds = _base(entity_type=entity_type).to_dataset()
        assert ds.job_run_params[key] == "limma"

    def test_does_not_emit_flat_de_method(self):
        """The wire format is entity-keyed — a flat ``de_method`` would be
        silently dropped by the MDFlexiComparisons schema and is therefore
        worse than useless."""
        ds = _base(entity_type="protein").to_dataset()
        assert "de_method" not in ds.job_run_params

    def test_does_not_emit_other_entities_de_method_fields(self):
        """When entity_type=protein, the wire should carry only
        ``de_method_protein`` — not the other four placeholders. The backend
        applies its own defaults for the others."""
        ds = _base(entity_type="protein").to_dataset()
        for k in (
            "de_method_peptide",
            "de_method_gene",
            "de_method_metabolite",
            "de_method_ptm",
        ):
            assert k not in ds.job_run_params

    def test_gene_with_edger_emits_edger_companion(self):
        pw = _base(entity_type="gene", de_method="edgeR")
        pw.validate()
        params = pw.to_dataset().job_run_params
        assert params["de_method_gene"] == "edgeR"
        assert params["edger_norm_method"] == "TMM"

    def test_gene_with_deseq2_emits_deseq2_companions(self):
        pw = _base(
            entity_type="gene",
            de_method="DESeq2",
            deseq2_lfc_shrinkage="apeglm",
            deseq2_alpha=0.1,
        )
        pw.validate()
        params = pw.to_dataset().job_run_params
        assert params["de_method_gene"] == "DESeq2"
        assert params["deseq2_lfc_shrinkage"] == "apeglm"
        assert params["deseq2_alpha"] == 0.1
        # apeglm_seed only when shrinkage=apeglm
        assert params["apeglm_seed"] == 1

    def test_deseq2_non_apeglm_omits_seed(self):
        pw = _base(
            entity_type="gene",
            de_method="DESeq2",
            deseq2_lfc_shrinkage="ashr",
        )
        pw.validate()
        params = pw.to_dataset().job_run_params
        assert "apeglm_seed" not in params

    def test_limma_omits_companion_params(self):
        params = _base(entity_type="gene", de_method="limma").to_dataset().job_run_params
        for k in (
            "edger_norm_method",
            "deseq2_lfc_shrinkage",
            "deseq2_alpha",
            "apeglm_seed",
        ):
            assert k not in params


class TestDeMethodValidation:
    @pytest.mark.parametrize(
        "entity_type",
        ["protein", "peptide", "metabolite", "ptm"],
    )
    def test_rejects_edger_for_non_gene_entity(self, entity_type):
        pw = _base(entity_type=entity_type, de_method="edgeR")
        with pytest.raises(ValueError, match="de_method 'edgeR' not allowed"):
            pw.validate()

    @pytest.mark.parametrize(
        "entity_type",
        ["protein", "peptide", "metabolite", "ptm"],
    )
    def test_rejects_deseq2_for_non_gene_entity(self, entity_type):
        pw = _base(entity_type=entity_type, de_method="DESeq2")
        with pytest.raises(ValueError, match="de_method 'DESeq2' not allowed"):
            pw.validate()

    def test_accepts_limma_for_every_entity(self):
        for entity in ("protein", "peptide", "gene", "metabolite", "ptm"):
            pw = _base(entity_type=entity, de_method="limma")
            pw.validate()  # no raise

    def test_gene_accepts_all_three_de_methods(self):
        for method in ("limma", "edgeR", "DESeq2"):
            _base(entity_type="gene", de_method=method).validate()

    def test_rejects_unknown_edger_norm_method(self):
        pw = _base(
            entity_type="gene",
            de_method="edgeR",
            edger_norm_method="quantile",
        )
        with pytest.raises(ValueError, match="edger_norm_method"):
            pw.validate()

    def test_rejects_unknown_deseq2_shrinkage(self):
        pw = _base(
            entity_type="gene",
            de_method="DESeq2",
            deseq2_lfc_shrinkage="bayes",
        )
        with pytest.raises(ValueError, match="deseq2_lfc_shrinkage"):
            pw.validate()

    def test_rejects_out_of_range_deseq2_alpha(self):
        pw = _base(
            entity_type="gene",
            de_method="DESeq2",
            deseq2_alpha=1.5,
        )
        with pytest.raises(ValueError, match="deseq2_alpha"):
            pw.validate()
