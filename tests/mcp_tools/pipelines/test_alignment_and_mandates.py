"""Lock in source-of-truth alignment and the LLM-behaviour mandates.

Two groups:

1. Alignment regression tests. Each test asserts the MCP tool emits the wire
   shape / default that the upstream Pydantic source-of-truth model expects.
   When upstream changes, these tests must be updated *deliberately*.

2. Mandate tests. Every analysis tool docstring must include the two-defaults
   mandate and the "ask the user before submitting" mandate. The mandates live
   in mcp_tools.pipelines._mandates.MANDATES_FRAGMENT (single source of truth).
"""

from unittest.mock import MagicMock

import pytest

from mcp_tools.health import _WORKFLOW_GUIDE
from mcp_tools.pipelines import (
    MANDATES_FRAGMENT,
    run_anova,
    run_dose_response,
    run_dose_response_bulk,
    run_dose_response_from_upload,
    run_normalisation_imputation,
    run_normalisation_imputation_bulk,
    run_pairwise_comparison,
    run_pairwise_comparison_bulk,
)
from mcp_tools.pipelines._schemas import _PIPELINE_SCHEMAS

from .conftest import INTENSITY_ID, OUTPUT_ID, SAMPLE_METADATA, patch_pipeline_client

# ──────────────────────────────────────────────────────────────────────────────
# 1. Alignment regression tests
# ──────────────────────────────────────────────────────────────────────────────


class TestAnovaConditionComparisonsShape:
    """ANOVA must wrap condition_comparisons as {"condition_comparison_pairs": [...]}.

    Source-of-truth: MDFlexiComparisons/src/md_flexi_comparisons/process_r.py:91-92
    (ConditionComparisons model). The ANOVAParamsProperties model expects this
    wrapper, not a bare list.
    """

    def test_custom_comparisons_are_wrapped(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                comparisons_type="custom",
                condition_comparisons=[["treated", "ctrl"], ["other", "ctrl"]],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["condition_comparisons"] == {
            "condition_comparison_pairs": [
                ["treated", "ctrl"],
                ["other", "ctrl"],
            ]
        }


class TestAnovaExtendedKwargs:
    """ANOVA tool now exposes control_variables and filter_threshold_count."""

    def test_control_variables_wrapped_on_wire(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                control_variables=[
                    {"column": "batch", "type": "categorical"},
                    {"column": "age", "type": "numerical"},
                ],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["control_variables"] == {
            "control_variables": [
                {"column": "batch", "type": "categorical"},
                {"column": "age", "type": "numerical"},
            ]
        }

    def test_filter_count_emitted_when_method_count(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_anova(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ANOVA",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                filter_method="count",
                filter_threshold_count=4,
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["filter_values_criteria"] == {
            "method": "count",
            "filter_threshold_count": 4,
        }
        # Percentage key must NOT be present.
        assert "filter_threshold_percentage" not in params["filter_values_criteria"]

    def test_filter_count_requires_threshold(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="filter_threshold_count"):
                run_anova(
                    input_dataset_ids=[INTENSITY_ID],
                    dataset_name="ANOVA",
                    sample_metadata=SAMPLE_METADATA,
                    condition_column="condition",
                    filter_method="count",
                )

    def test_bad_control_variable_shape_raises(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            with pytest.raises(ValueError, match="control_variables"):
                run_anova(
                    input_dataset_ids=[INTENSITY_ID],
                    dataset_name="ANOVA",
                    sample_metadata=SAMPLE_METADATA,
                    condition_column="condition",
                    control_variables=[{"column": "batch"}],  # missing 'type'
                )


class TestPairwiseGeneFitSeparateModelsWarning:
    """Gene + fit_separate_models=True must surface a warning to the LLM."""

    def test_warning_prepended_to_prose(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="gene pw",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                entity_type="gene",
                fit_separate_models=True,
            )

        assert "GENE PAIRWISE WARNING" in result
        assert "fit_separate_models=False" in result
        # Sentinel must remain so bulk parsing still works.
        assert f"Dataset ID: {OUTPUT_ID}" in result
        # Bulk parser uses split("Dataset ID:")[-1].strip().
        assert result.split("Dataset ID:")[-1].strip() == OUTPUT_ID

    def test_no_warning_when_gene_with_separate_models_false(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="gene pw ok",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                entity_type="gene",
                fit_separate_models=False,
            )
        assert "GENE PAIRWISE WARNING" not in result

    def test_no_warning_for_protein_with_separate_models_true(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="protein pw",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                entity_type="protein",
                fit_separate_models=True,
            )
        assert "GENE PAIRWISE WARNING" not in result


class TestSchemasModuleDocstring:
    """Ensure the _schemas.py module docstring no longer claims a 'state of flux'.

    The old text described the wire format as transitioning between an old
    nested form and a new flat one — that contradicts what NI v3 actually does
    and could confuse the LLM.
    """

    def test_module_docstring_describes_flat_wire_format(self):
        from mcp_tools.pipelines import _schemas

        doc = _schemas.__doc__ or ""
        assert "state of flux" not in doc
        assert "flat" in doc.lower()
        assert "canonical" in doc.lower()


class TestDoseResponseUseImputedDefault:
    """Platform default for use_imputed_intensities is False.

    Source-of-truth: data-set-service/src/flows/utils/type_defs.py:78
    DoseResponseParams.use_imputed_intensities Field(..., default=False).
    """

    def test_dose_response_default_is_false(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_dose_response(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="DR",
                sample_names=["s1", "s2", "s3", "s4"],
                control_samples=["s1", "s2"],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["use_imputed_intensities"] is False

    def test_schema_use_imputed_default_is_false(self):
        params = _PIPELINE_SCHEMAS["dose_response"]["parameters"]
        assert params["use_imputed_intensities"]["default"] is False


class TestPairwiseControlVariablesShape:
    """control_variables on the wire is {"control_variables": [{"column", "type"}]}.

    Source-of-truth: MDFlexiComparisons ControlValue and ControlVariables models
    (process_r.py:69-75). The MCP wraps the user-supplied list automatically.
    """

    def test_control_variables_wrapped_on_wire(self):
        from md_python.models.metadata import SampleMetadata

        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_pairwise_comparison(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="PC",
                sample_metadata=SAMPLE_METADATA,
                condition_column="condition",
                condition_comparisons=[["treated", "ctrl"]],
                control_variables=[{"column": "batch", "type": "categorical"}],
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["control_variables"] == {
            "control_variables": [{"column": "batch", "type": "categorical"}]
        }
        # Smoke check that SampleMetadata import path is fine
        assert SampleMetadata is not None  # noqa: B015


class TestUploadSourceEnumAlignment:
    """The MCP source enum must be a subset of the workflow's VALID_SOURCE_FORMATS.

    Source-of-truth: workflow/app/models/experiment.rb:27-35 (VALID_SOURCE_FORMATS).
    """

    def test_mcp_sources_are_subset_of_workflow_sources(self):
        from md_python.resources.v2.uploads import ALLOWED_UPLOAD_SOURCES

        # Workflow VALID_SOURCE_FORMATS as of experiment.rb:28-37.
        workflow_sources = frozenset(
            {
                "diann_tabular",
                "tims_diann",
                "spectronaut",
                "maxquant",
                "md_format",
                "md_format_gene",
                "md_format_metabolite",
                "unknown",
            }
        )
        assert ALLOWED_UPLOAD_SOURCES <= workflow_sources

    def test_mcp_sources_include_gene_format(self):
        from md_python.resources.v2.uploads import ALLOWED_UPLOAD_SOURCES

        assert "md_format_gene" in ALLOWED_UPLOAD_SOURCES

    def test_mcp_sources_include_metabolite_format(self):
        from md_python.resources.v2.uploads import ALLOWED_UPLOAD_SOURCES

        assert "md_format_metabolite" in ALLOWED_UPLOAD_SOURCES


class TestNormalisationSchemaEntityValuesAlignment:
    """valid_values_per_entity_type must match the converter's per-entity Literals.

    Source-of-truth: md-converter/src/flows/intensity_imputation_types.py
    (NormalisationAndImputationParamsProperties).
    """

    def test_normalisation_methods_per_entity_type(self):
        params = _PIPELINE_SCHEMAS["normalisation_imputation"]["parameters"]
        per_entity = params["normalisation_method"]["valid_values_per_entity_type"]
        # Converter normalisation_methods_proteomics literal: skip|median|batch
        # correction|quantile|sum (intensity_imputation_types.py:558-564).
        assert set(per_entity["protein"]) == {
            "skip",
            "median",
            "quantile",
            "sum",
            "batch correction",
        }
        assert set(per_entity["peptide"]) == {
            "skip",
            "median",
            "quantile",
            "sum",
            "batch correction",
        }
        # Converter normalisation_methods_gene Literal also includes 'cpm'
        # (intensity_imputation_types.py:538-545).
        assert set(per_entity["gene"]) == {
            "skip",
            "median",
            "quantile",
            "sum",
            "batch correction",
            "cpm",
        }

    def test_filtration_methods_per_entity_type(self):
        params = _PIPELINE_SCHEMAS["normalisation_imputation"]["parameters"]
        per_entity = params["filtration_method"]["valid_values_per_entity_type"]
        # filtration_methods_protein literal: skip | by missing values
        # (intensity_imputation_types.py:400-407).
        assert set(per_entity["protein"]) == {"skip", "by missing values"}
        # filtration_methods_peptide literal:
        # skip | by ptm localization probability | by missing values (:383-390).
        assert set(per_entity["peptide"]) == {
            "skip",
            "by missing values",
            "by ptm localization probability",
        }
        # filtration_methods_gene literal: skip | by minimum abundance (:367-374).
        assert set(per_entity["gene"]) == {"skip", "by minimum abundance"}

    def test_batch_correction_techniques_per_entity_type(self):
        params = _PIPELINE_SCHEMAS["normalisation_imputation"]["parameters"]
        bc = params["normalisation_method"]["method_params"]["batch correction"]
        per_entity = bc["batch_correction_technique"]["valid_values_per_entity_type"]
        # batch_correction_technique_proteomics: limma|combat (:578-583).
        assert set(per_entity["protein"]) == {
            "limma remove batch effect",
            "combat",
        }
        assert set(per_entity["peptide"]) == {
            "limma remove batch effect",
            "combat",
        }
        # batch_correction_technique_gene: limma|combat|combat seq (:594-600).
        assert set(per_entity["gene"]) == {
            "limma remove batch effect",
            "combat",
            "combat seq",
        }


class TestNormalisationDefaultsAlignment:
    """Defaults emitted client-side must match the converter Pydantic defaults."""

    def test_filter_threshold_proportion_default(self):
        # Converter NormalisationAndImputationParamsProperties.filter_threshold_proportion
        # default=0.5 (intensity_imputation_types.py:454-465).
        from mcp_tools.pipelines.normalisation import _FILT_DEFAULTS

        for method in ("by missing values", "by minimum abundance"):
            assert _FILT_DEFAULTS[method]["filter_threshold_proportion"] == 0.5

    def test_median_centre_at_zero_default(self):
        # Converter median_normalisation_centre_at_zero default=True (:709-718).
        from mcp_tools.pipelines.normalisation import _NORM_DEFAULTS

        assert _NORM_DEFAULTS["median"]["median_normalisation_centre_at_zero"] is True

    def test_include_imputed_values_default(self):
        # Converter include_imputed_values default=False (:693-707).
        from mcp_tools.pipelines.normalisation import _NORM_DEFAULTS

        for method in ("median", "quantile", "sum", "batch correction"):
            assert _NORM_DEFAULTS[method]["include_imputed_values"] is False

    def test_imputation_mnar_default(self):
        # Converter MNAR std_position=1.8, std_width=0.3 (:780-801).
        from mcp_tools.pipelines.normalisation import _IMP_DEFAULTS

        assert _IMP_DEFAULTS["mnar"]["std_position"] == 1.8
        assert _IMP_DEFAULTS["mnar"]["std_width"] == 0.3

    def test_imputation_knn_tn_default(self):
        # Converter knn_tn knn_tn_k=5, knn_tn_distance="truncation" (:823-848).
        from mcp_tools.pipelines.normalisation import _IMP_DEFAULTS

        assert _IMP_DEFAULTS["knn_tn"]["knn_tn_k"] == 5
        assert _IMP_DEFAULTS["knn_tn"]["knn_tn_distance"] == "truncation"


# ──────────────────────────────────────────────────────────────────────────────
# 2. Mandate tests — every analysis tool docstring carries both mandates
# ──────────────────────────────────────────────────────────────────────────────


_ANALYSIS_TOOLS = [
    run_normalisation_imputation,
    run_normalisation_imputation_bulk,
    run_pairwise_comparison,
    run_pairwise_comparison_bulk,
    run_anova,
    run_dose_response,
    run_dose_response_from_upload,
    run_dose_response_bulk,
]


@pytest.mark.parametrize("tool", _ANALYSIS_TOOLS, ids=lambda t: t.__name__)
def test_tool_docstring_includes_mandates_block(tool):
    """The MANDATES_FRAGMENT block must be present verbatim in every analysis tool."""
    doc = tool.__doc__ or ""
    assert (
        "LLM BEHAVIOURAL MANDATES" in doc
    ), f"{tool.__name__} docstring must include the LLM BEHAVIOURAL MANDATES block."


@pytest.mark.parametrize("tool", _ANALYSIS_TOOLS, ids=lambda t: t.__name__)
def test_tool_docstring_includes_ask_the_user_mandate(tool):
    """Every analysis tool must instruct the LLM to ask the user before submitting."""
    doc = tool.__doc__ or ""
    assert (
        "MANDATORY PARAMETER Q&A" in doc
    ), f"{tool.__name__} docstring must include the MANDATORY PARAMETER Q&A clause."


@pytest.mark.parametrize("tool", _ANALYSIS_TOOLS, ids=lambda t: t.__name__)
def test_tool_docstring_includes_two_defaults_mandate(tool):
    """Every analysis tool must mandate the two-defaults presentation."""
    doc = tool.__doc__ or ""
    assert (
        "TWO-DEFAULTS MANDATE" in doc
    ), f"{tool.__name__} docstring must include the TWO-DEFAULTS MANDATE."
    assert "PLATFORM DEFAULT" in doc and "LLM RECOMMENDATION" in doc, (
        f"{tool.__name__} docstring must name both the PLATFORM DEFAULT and "
        f"the LLM RECOMMENDATION columns."
    )


def test_mandates_fragment_names_both_columns():
    """Belt-and-braces sanity check on the shared fragment."""
    assert "PLATFORM DEFAULT" in MANDATES_FRAGMENT
    assert "LLM RECOMMENDATION" in MANDATES_FRAGMENT
    assert "MANDATORY PARAMETER Q&A" in MANDATES_FRAGMENT
    assert "TWO-DEFAULTS MANDATE" in MANDATES_FRAGMENT


def test_workflow_guide_includes_analysis_mandates_section():
    """get_workflow_guide must surface the mandates so an LLM can find them quickly."""
    assert "analysis_mandates" in _WORKFLOW_GUIDE
    mandates = _WORKFLOW_GUIDE["analysis_mandates"]
    assert any("MANDATORY Q&A" in m for m in mandates)
    assert any("TWO-DEFAULTS MANDATE" in m for m in mandates)


def test_common_mistakes_includes_two_defaults_mandate():
    """common_mistakes must include the two-defaults mandate (strengthens the prior
    'never auto-pick' entry)."""
    mistakes = _WORKFLOW_GUIDE["common_mistakes"]
    assert any("TWO-DEFAULTS" in m for m in mistakes)
    assert any("MANDATORY Q&A" in m for m in mistakes)
