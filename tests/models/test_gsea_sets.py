"""Tests for the species-conditional CAMERA GSEA `sets` enum.

Regression cover for the silent-drop bug: the backend accepts an unrecognised
`sets` value, reports COMPLETED, and never runs that knowledge base (dataset
ce724081-a9fd-4284-a0a3-e3630679d001 requested 4 sets including "Hallmark" and
ran 3). Unknown values must now fail fast, never be dropped.
"""

import pytest

from md_python.models.dataset_builders._gsea_sets import (
    GSEA_SETS_BY_SPECIES,
    GseaSetsValidator,
)
from md_python.models.dataset_builders.gsea import GseaDataset
from md_python.models.metadata import SampleMetadata

ONE_ID = ["11111111-1111-1111-1111-111111111111"]


@pytest.fixture
def validator() -> GseaSetsValidator:
    return GseaSetsValidator()


def _gsea(species: str, sets=None) -> GseaDataset:
    return GseaDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="GSEA",
        sample_metadata=SampleMetadata(
            data=[["sample_name", "condition"], ["s1", "a"], ["s2", "b"]]
        ),
        condition_column="condition",
        condition_comparisons=[["a", "b"]],
        species=species,
        sets=sets,
    )


class TestCatalogue:
    def test_species_and_counts_match_the_live_catalogue(self):
        assert {
            species: len(values) for species, values in GSEA_SETS_BY_SPECIES.items()
        } == {"Human": 14, "Mouse": 11, "Yeast": 4, "Chinese hamster": 3}

    def test_mouse_uses_m_prefixes_not_human_c_numbers(self):
        mouse = GSEA_SETS_BY_SPECIES["Mouse"]
        assert "MSigDB-MH (hallmark gene sets)" in mouse
        assert "MSigDB-M2 (curated gene sets)" in mouse
        assert not any(v.startswith("MSigDB-C") for v in mouse)

    def test_chinese_hamster_has_no_reactome(self):
        assert "Reactome" not in GSEA_SETS_BY_SPECIES["Chinese hamster"]
        assert "Reactome" in GSEA_SETS_BY_SPECIES["Yeast"]


class TestAcceptsValidSets:
    @pytest.mark.parametrize("species", sorted(GSEA_SETS_BY_SPECIES))
    def test_every_value_of_every_species_is_accepted(self, species, validator):
        values = GSEA_SETS_BY_SPECIES[species]
        assert validator.canonicalise(values, species) == values

    @pytest.mark.parametrize("species", sorted(GSEA_SETS_BY_SPECIES))
    def test_builder_accepts_the_whole_species_list(self, species):
        params = _gsea(species, GSEA_SETS_BY_SPECIES[species]).to_dataset()
        assert params.job_run_params["sets"] == GSEA_SETS_BY_SPECIES[species]

    def test_default_sets_are_the_three_go_sets(self):
        assert _gsea("Human").to_dataset().job_run_params["sets"] == [
            "GO - Biological Process",
            "GO - Cellular Component",
            "GO - Molecular Function",
        ]

    @pytest.mark.parametrize(
        "given,expected",
        [
            ("  Reactome  ", "Reactome"),
            ("reactome", "Reactome"),
            ("go - biological process", "GO - Biological Process"),
            ("GO -  Biological  Process", "GO - Biological Process"),
            ("msigdb-h (HALLMARK GENE SETS)", "MSigDB-H (hallmark gene sets)"),
        ],
    )
    def test_case_and_whitespace_variants_are_normalised(
        self, given, expected, validator
    ):
        assert validator.canonicalise([given], "Human") == [expected]

    def test_duplicates_collapse_preserving_order(self, validator):
        result = validator.canonicalise(["Reactome", "reactome", "Reactome"], "Human")
        assert result == ["Reactome"]


class TestRejectsUnknownSets:
    def test_unknown_value_names_the_offender_and_the_species_list(self, validator):
        with pytest.raises(ValueError) as exc:
            validator.canonicalise(["GO - Biological Process", "Wombat"], "Human")
        msg = str(exc.value)
        assert "'Wombat'" in msg
        assert "Human" in msg
        for value in GSEA_SETS_BY_SPECIES["Human"]:
            assert value in msg

    @pytest.mark.parametrize(
        "species,expected",
        [
            ("Human", "MSigDB-H (hallmark gene sets)"),
            ("Mouse", "MSigDB-MH (hallmark gene sets)"),
        ],
    )
    def test_hallmark_shorthand_gets_the_species_correct_hint(
        self, species, expected, validator
    ):
        with pytest.raises(ValueError) as exc:
            validator.canonicalise(["Hallmark"], species)
        assert expected in str(exc.value)

    @pytest.mark.parametrize(
        "shorthand", ["Hallmark", "MSigDB-H", "hallmark gene sets"]
    )
    def test_hallmark_shorthands_are_rejected_not_aliased(self, shorthand, validator):
        with pytest.raises(ValueError, match="not valid knowledge bases"):
            validator.canonicalise([shorthand], "Human")

    def test_the_confirmed_live_bug_payload_is_rejected(self):
        """The exact sets list of dataset ce724081 — 4 requested, 3 silently ran."""
        with pytest.raises(ValueError) as exc:
            _gsea(
                "Human",
                [
                    "GO - Biological Process",
                    "GO - Cellular Component",
                    "GO - Molecular Function",
                    "Hallmark",
                ],
            ).validate()
        msg = str(exc.value)
        assert "'Hallmark'" in msg
        assert "MSigDB-H (hallmark gene sets)" in msg

    def test_human_only_value_rejected_under_mouse_with_the_mouse_list(self, validator):
        with pytest.raises(ValueError) as exc:
            validator.canonicalise(["MSigDB-C2 (curated gene sets)"], "Mouse")
        msg = str(exc.value)
        assert "MSigDB-M2 (curated gene sets)" in msg  # species-correct hint
        assert "'Mouse'" in msg
        assert "MSigDB-C4" not in msg  # the Human list is not offered

    def test_human_only_collection_absent_from_mouse_is_named_as_such(self, validator):
        with pytest.raises(ValueError) as exc:
            validator.canonicalise(
                ["MSigDB-C6 (oncogenic signature gene sets)"], "Mouse"
            )
        assert "not available for 'Mouse'" in str(exc.value)

    def test_reactome_rejected_for_chinese_hamster(self, validator):
        with pytest.raises(ValueError) as exc:
            validator.canonicalise(["Reactome"], "Chinese hamster")
        assert "not available for 'Chinese hamster'" in str(exc.value)

    def test_empty_sets_rejected(self, validator):
        with pytest.raises(ValueError, match="sets cannot be empty"):
            validator.canonicalise([], "Human")

    def test_unknown_species_rejected(self, validator):
        with pytest.raises(ValueError, match="species must be one of"):
            validator.canonicalise(["Reactome"], "human")

    def test_non_string_values_rejected(self, validator):
        with pytest.raises(ValueError, match="sets must be a list of strings"):
            validator.canonicalise(["Reactome", 7], "Human")  # type: ignore[list-item]

    def test_builder_never_ships_an_unknown_set_to_the_wire(self):
        with pytest.raises(ValueError):
            _gsea("Human", ["Reactome", "Hallmark"]).to_dataset()
