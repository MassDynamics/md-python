"""Tests for run_normalisation_imputation."""

from unittest.mock import MagicMock, patch

import pytest

from mcp_tools.pipelines import run_normalisation_imputation
from mcp_tools.pipelines.normalisation import _submit_ni_job

from .conftest import INTENSITY_ID, OUTPUT_ID, patch_pipeline_client


class TestRunNormalisationImputation:
    def test_basic_run(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="median",
                imputation_method="mnar",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_entity_type_at_top_level(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="median",
                imputation_method="mnar",
                entity_type="peptide",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        # Flat v2 schema: entity_type at top level, methods as scalar strings.
        assert params["entity_type"] == "peptide"
        assert params["normalisation_methods_proteomics"] == "median"
        assert params["imputation_methods"] == "mnar"

    def test_extra_params_merged(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="My Norm",
                normalisation_method="quantile",
                imputation_method="knn",
                normalisation_extra_params={"reference": "global"},
                imputation_extra_params={"k": 5},
            )

        call_args = mock_client.datasets.create.call_args[0][0]
        params = call_args.job_run_params
        assert params["normalisation_methods_proteomics"] == "quantile"
        assert params["imputation_methods"] == "knn"
        assert params["reference"] == "global"
        assert params["k"] == 5

    def test_combat_passes_through_keys(self):
        """Phase B: combat sub-technique flows through to job_run_params."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="combat protein",
                normalisation_method="batch correction",
                imputation_method="skip",
                entity_type="protein",
                normalisation_extra_params={
                    "batch_correction_technique": "combat",
                    "batch_variable_combat": "batch",
                    "mean_only": True,
                    "experiment_design": {
                        "sample_name": ["s1", "s2"],
                        "batch": ["a", "b"],
                    },
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["normalisation_methods_proteomics"] == "batch correction"
        assert params["batch_correction_technique_proteomics"] == "combat"
        assert params["batch_variable_combat"] == "batch"
        assert params["mean_only"] is True
        assert "batch_variables" not in params

    def test_combat_seq_for_gene(self):
        """Gene + combat seq emits batch_correction_technique_gene."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="combat seq gene",
                normalisation_method="batch correction",
                imputation_method="skip",
                entity_type="gene",
                normalisation_extra_params={
                    "batch_correction_technique": "combat seq",
                    "batch_variable_combat": "batch",
                    "experiment_design": {
                        "sample_name": ["s1", "s2"],
                        "batch": ["a", "b"],
                    },
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["batch_correction_technique_gene"] == "combat seq"
        # combat seq does not accept mean_only / reference_batch_combat
        assert "mean_only" not in params
        assert "reference_batch_combat" not in params

    def test_knn_tn_defaults_applied(self):
        """knn_tn without overrides applies the converter defaults."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="knn_tn defaults",
                normalisation_method="skip",
                imputation_method="knn_tn",
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["imputation_methods"] == "knn_tn"
        assert params["knn_tn_k"] == 5
        assert params["knn_tn_distance"] == "truncation"

    def test_filter_only_pattern(self):
        """skip + skip + by missing values produces a filtration-only payload."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="protein filter only",
                normalisation_method="skip",
                imputation_method="skip",
                entity_type="protein",
                filtration_method="by missing values",
                filtration_extra_params={
                    "filter_valid_values_criteria": "percentage",
                    "filter_threshold_proportion": 0.5,
                    "filter_valid_values_logic": "at least one condition",
                    "filter_based_on_condition": "condition",
                    "experiment_design": {
                        "sample_name": ["s1", "s2", "s3", "s4"],
                        "condition": ["a", "a", "b", "b"],
                    },
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["normalisation_methods_proteomics"] == "skip"
        assert params["imputation_methods"] == "skip"
        assert params["filtration_methods_protein"] == "by missing values"
        assert params["filter_valid_values_criteria"] == "percentage"
        assert params["filter_based_on_condition"] == "condition"

    def test_gene_workflow_with_cpm_and_minimum_abundance(self):
        """Gene + cpm + by minimum abundance + skip imputation."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="gene NI",
                normalisation_method="cpm",
                imputation_method="skip",
                entity_type="gene",
                filtration_method="by minimum abundance",
                normalisation_extra_params={"prior_count": 2},
                filtration_extra_params={
                    "minimum_abundance_threshold": 1.0,
                    "filter_valid_values_criteria": "percentage",
                    "filter_threshold_proportion": 0.5,
                    "filter_valid_values_logic": "full experiment",
                    "experiment_design": {
                        "sample_name": ["s1", "s2"],
                        "condition": ["a", "b"],
                    },
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["entity_type"] == "gene"
        assert params["normalisation_methods_gene"] == "cpm"
        assert params["filtration_methods_gene"] == "by minimum abundance"
        assert params["minimum_abundance_threshold"] == 1.0
        assert params["prior_count"] == 2

    def test_legacy_underscore_method_aliased_to_canonical(self):
        """Legacy 'batch_correction' input emits canonical 'batch correction'."""
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="legacy alias",
                normalisation_method="batch_correction",
                imputation_method="skip",
                entity_type="protein",
                normalisation_extra_params={
                    "batch_correction_technique": "limma_remove_batch_effect",
                    "batch_variables": [{"column": "batch", "type": "categorical"}],
                    "experiment_design": {"sample_name": ["s1"], "batch": ["a"]},
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["normalisation_methods_proteomics"] == "batch correction"
        assert (
            params["batch_correction_technique_proteomics"]
            == "limma remove batch effect"
        )


# get_dataset returns experiment_design ROW-oriented (header row + data rows).
# The tool accepts that shape as well as the column-oriented wire dict.
_ROW_DESIGN = [
    ["sample_name", "condition", "cell_line"],
    ["A2780_rep1", "DMSO", "A2780"],
    ["A2780_rep2", "DMSO", "A2780"],
    ["A2780_rep3", "Drug", "A2780"],
    ["A2780_rep4", "Drug", "A2780"],
]
_COLUMN_DESIGN = {
    "sample_name": ["A2780_rep1", "A2780_rep2", "A2780_rep3", "A2780_rep4"],
    "condition": ["DMSO", "DMSO", "Drug", "Drug"],
    "cell_line": ["A2780", "A2780", "A2780", "A2780"],
}


class TestExperimentDesignShapes:
    """Both experiment_design shapes reach the wire as the column dict."""

    @pytest.mark.parametrize(
        "design", [_ROW_DESIGN, _COLUMN_DESIGN], ids=["row_oriented", "column_dict"]
    )
    def test_filtration_experiment_design_accepts_both_shapes(self, design):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            result = run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="ed shape",
                normalisation_method="median",
                imputation_method="mnar",
                entity_type="protein",
                filtration_method="by missing values",
                filtration_extra_params={
                    "filter_based_on_condition": "condition",
                    "experiment_design": design,
                },
            )

        assert OUTPUT_ID in result
        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["experiment_design"] == _COLUMN_DESIGN

    def test_batch_correction_experiment_design_accepts_row_shape(self):
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch_pipeline_client(mock_client):
            run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="bc row ed",
                normalisation_method="batch correction",
                imputation_method="skip",
                entity_type="protein",
                normalisation_extra_params={
                    "batch_correction_technique": "combat",
                    "batch_variable_combat": "cell_line",
                    "experiment_design": _ROW_DESIGN,
                },
            )

        params = mock_client.datasets.create.call_args[0][0].job_run_params
        assert params["experiment_design"] == _COLUMN_DESIGN

    @pytest.mark.parametrize(
        "bad_design,expected_fragment",
        [
            ([], "cannot be empty"),
            (
                [["sample_name", "condition"], ["s1", "a"], ["s2"]],
                "rows must not be ragged",
            ),
            ([["sample_name", "condition"]], "header row but no sample rows"),
            (["sample_name", "condition"], "entries are not rows"),
            ("sample_name,condition", "unsupported type 'str'"),
        ],
        ids=[
            "empty_list",
            "ragged_rows",
            "header_only",
            "flat_list_of_strings",
            "not_a_list_or_dict",
        ],
    )
    def test_malformed_experiment_design_returns_error_envelope(
        self, bad_design, expected_fragment
    ):
        """A bad shape returns the prose error envelope naming both accepted
        shapes — not a raw pydantic ValidationError."""
        mock_client = MagicMock()

        with patch_pipeline_client(mock_client):
            result = run_normalisation_imputation(
                input_dataset_ids=[INTENSITY_ID],
                dataset_name="bad ed",
                normalisation_method="median",
                imputation_method="mnar",
                filtration_method="by missing values",
                filtration_extra_params={
                    "filter_based_on_condition": "condition",
                    "experiment_design": bad_design,
                },
            )

        assert result.startswith("Error: ")
        assert expected_fragment in result
        assert "column-oriented dict" in result
        assert "row-oriented list of lists" in result
        assert "\n" not in result  # flattened, not the raw pydantic rendering
        mock_client.datasets.create.assert_not_called()


class TestConditionalRequirementErrors:
    """Conditional-required params surface as the prose error envelope, never as
    an uncaught exception. Each message names the missing param, the trigger,
    and what to pass. All four cases below come from live MCP telemetry."""

    def _run(self, **kwargs) -> tuple[str, MagicMock]:
        mock_client = MagicMock()
        mock_client.datasets.create.return_value = OUTPUT_ID
        with patch_pipeline_client(mock_client):
            result = run_normalisation_imputation(**kwargs)
        return result, mock_client

    def test_experiment_design_required_for_minimum_abundance(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="gene no ed",
            normalisation_method="cpm",
            imputation_method="skip",
            entity_type="gene",
            filtration_method="by minimum abundance",
            filtration_extra_params={"filter_based_on_condition": "condition"},
        )

        assert result.startswith("Error: ")
        assert "experiment_design is required" in result
        assert "filtration_method='by minimum abundance'" in result
        assert "filtration_extra_params" in result
        mock_client.datasets.create.assert_not_called()

    def test_experiment_design_required_for_by_missing_values(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="protein no ed",
            normalisation_method="median",
            imputation_method="mnar",
            filtration_method="by missing values",
            filtration_extra_params={"filter_based_on_condition": "condition"},
        )

        assert result.startswith("Error: ")
        assert "experiment_design is required" in result
        assert "filtration_method='by missing values'" in result
        mock_client.datasets.create.assert_not_called()

    def test_filter_based_on_condition_required_for_condition_logic(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="no condition col",
            normalisation_method="median",
            imputation_method="mnar",
            filtration_method="by missing values",
            filtration_extra_params={"experiment_design": _ROW_DESIGN},
        )

        assert result.startswith("Error: ")
        assert "filter_based_on_condition (non-empty str) is required" in result
        assert "filter_valid_values_logic='at least one condition'" in result
        assert "full experiment" in result  # the escape hatch is named
        mock_client.datasets.create.assert_not_called()

    def test_experiment_design_required_for_batch_correction(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="bc no ed",
            normalisation_method="batch correction",
            imputation_method="skip",
            normalisation_extra_params={
                "batch_correction_technique": "combat",
                "batch_variable_combat": "batch",
            },
        )

        assert result.startswith("Error: ")
        assert "experiment_design is required" in result
        assert "normalisation_method='batch correction'" in result
        mock_client.datasets.create.assert_not_called()

    def test_batch_correction_technique_required(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="bc no technique",
            normalisation_method="batch correction",
            imputation_method="skip",
            normalisation_extra_params={"experiment_design": _COLUMN_DESIGN},
        )

        assert result.startswith("Error: ")
        assert "batch_correction_technique is required" in result
        assert "normalisation_method='batch correction'" in result
        assert "limma remove batch effect" in result  # allowed values listed
        mock_client.datasets.create.assert_not_called()

    def test_batch_variables_required_for_limma(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="limma no vars",
            normalisation_method="batch correction",
            imputation_method="skip",
            normalisation_extra_params={
                "batch_correction_technique": "limma remove batch effect",
                "experiment_design": _COLUMN_DESIGN,
            },
        )

        assert result.startswith("Error: ")
        assert "batch_variables (non-empty list) is required" in result
        assert "batch_correction_technique='limma remove batch effect'" in result
        assert "'type': 'categorical'" in result  # what to pass
        mock_client.datasets.create.assert_not_called()

    def test_batch_variable_combat_required_for_combat(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="combat no var",
            normalisation_method="batch correction",
            imputation_method="skip",
            normalisation_extra_params={
                "batch_correction_technique": "combat",
                "experiment_design": _COLUMN_DESIGN,
            },
        )

        assert result.startswith("Error: ")
        assert "batch_variable_combat (non-empty str) is required" in result
        assert "batch_correction_technique='combat'" in result
        mock_client.datasets.create.assert_not_called()

    def test_filter_valid_values_criteria_required(self):
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="no criteria",
            normalisation_method="median",
            imputation_method="mnar",
            filtration_method="by missing values",
            filtration_extra_params={
                "filter_valid_values_criteria": None,
                "filter_based_on_condition": "condition",
                "experiment_design": _COLUMN_DESIGN,
            },
        )

        assert result.startswith("Error: ")
        assert "filter_valid_values_criteria is required" in result
        assert "filtration_method='by missing values'" in result
        mock_client.datasets.create.assert_not_called()

    def test_invalid_enum_value_also_returns_envelope(self):
        """Any local ValueError — not just conditional-required ones — is caught."""
        result, mock_client = self._run(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="bad method",
            normalisation_method="cpm",  # gene-only
            imputation_method="mnar",
            entity_type="protein",
        )

        assert result.startswith("Error: ")
        assert "normalisation_method 'cpm' not allowed" in result
        mock_client.datasets.create.assert_not_called()

    def test_api_error_still_propagates(self):
        """Server-side failures are NOT swallowed by the local-validation guard."""
        mock_client = MagicMock()
        mock_client.datasets.create.side_effect = RuntimeError("APIError 422")

        with patch_pipeline_client(mock_client):
            with pytest.raises(RuntimeError, match="APIError 422"):
                run_normalisation_imputation(
                    input_dataset_ids=[INTENSITY_ID],
                    dataset_name="server boom",
                    normalisation_method="median",
                    imputation_method="mnar",
                )


class TestBulkSurfacesValidationErrors:
    def test_bulk_marks_invalid_params_job_as_failed(self):
        """The prose error envelope must map back onto the bulk error envelope —
        otherwise an invalid job would be miscounted as submitted."""
        mock_client = MagicMock()
        with patch_pipeline_client(mock_client):
            entry = _submit_ni_job(
                0,
                {
                    "upload_id": "u1",
                    "dataset_name": "bad job",
                    "normalisation_method": "median",
                    "imputation_method": "mnar",
                    "filtration_method": "by missing values",
                    "if_exists": "run",
                },
                existing_cache={},
                initial_ds_cache={"u1": INTENSITY_ID},
            )

        assert entry["error_code"] == "invalid_params"
        assert "filter_based_on_condition (non-empty str) is required" in entry["error"]
        assert not entry["error"].startswith("Error: ")  # sentinel stripped
        assert "dataset_id" not in entry


class TestConditionalRequirementDocs:
    def test_docstring_documents_conditional_requirements(self):
        doc = run_normalisation_imputation.__doc__ or ""
        assert "CONDITIONAL REQUIREMENTS" in doc
        assert "EXPERIMENT_DESIGN SHAPES" in doc
        for param in (
            "batch_correction_technique",
            "batch_variables",
            "batch_variable_combat",
            "filter_valid_values_criteria",
            "filter_based_on_condition",
        ):
            assert param in doc
