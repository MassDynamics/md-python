"""Tests for run_normalisation_imputation."""

from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import run_normalisation_imputation

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
