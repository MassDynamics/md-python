"""Tests for *_bulk pipeline submission tools."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import (
    run_dose_response_bulk,
    run_normalisation_imputation_bulk,
    run_pairwise_comparison_bulk,
)

from .conftest import (
    OUTPUT_ID,
    mock_dr_ds,
    mock_initial_ds,
    mock_initial_ds_dataset,
    patch_pipeline_client,
)


class TestRunDoseResponseBulk:
    def test_runs_all_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [mock_initial_ds_dataset()]
        mock_client.datasets.create.side_effect = ["dr-id-1", "dr-id-2"]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": "DR A",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
            {
                "upload_id": "upload-1",
                "dataset_name": "DR B",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
        ]

        with patch_pipeline_client(mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        assert result[0]["dataset_id"] == "dr-id-1"
        assert result[1]["dataset_id"] == "dr-id-2"

    def test_skips_existing_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [
            mock_dr_ds(dataset_id="existing-id", name="DR A")
        ]

        with patch_pipeline_client(mock_client):
            result = json.loads(
                run_dose_response_bulk(
                    [
                        {
                            "upload_id": "upload-1",
                            "dataset_name": "DR A",
                            "sample_names": ["s1"],
                            "control_samples": ["s1"],
                        }
                    ]
                )
            )

        assert result[0]["dataset_id"] == "existing-id"
        assert result[0]["skipped"] is True
        mock_client.datasets.create.assert_not_called()

    def test_caches_initial_dataset_lookup(self):
        """list_by_upload is called once per unique upload_id (prefetch phase)."""
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [mock_initial_ds_dataset()]
        mock_client.datasets.create.side_effect = ["id-1", "id-2", "id-3"]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": f"DR {i}",
                "sample_names": ["s1"],
                "control_samples": ["s1"],
                "if_exists": "run",
            }
            for i in range(3)
        ]

        with patch_pipeline_client(mock_client):
            run_dose_response_bulk(jobs)

        mock_client.datasets.list_by_upload.assert_called_once_with("upload-1")

    def test_enforces_job_cap(self):
        """Returns error JSON when more than 500 jobs are submitted."""
        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": f"DR {i}",
                "sample_names": ["s1"],
                "control_samples": ["s1"],
            }
            for i in range(501)
        ]
        result = json.loads(run_dose_response_bulk(jobs))
        assert "error" in result
        assert "500" in result["error"]

    def test_auto_fetches_and_caches_upload_metadata(self):
        """Upload metadata is fetched once per upload_id and filtered per job."""
        mock_upload = MagicMock()
        mock_upload.sample_metadata.data = [
            ["sample_name", "dose"],
            ["s1", "0"],
            ["s2", "10"],
            ["s3", "20"],
        ]
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [mock_initial_ds_dataset()]
        mock_client.datasets.create.side_effect = ["id-1", "id-2"]
        mock_client.uploads.get_by_id.return_value = mock_upload

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": "DR A",
                "sample_names": ["s1", "s2"],
                "control_samples": ["s1"],
                "if_exists": "run",
            },
            {
                "upload_id": "upload-1",
                "dataset_name": "DR B",
                "sample_names": ["s2", "s3"],
                "control_samples": ["s2"],
                "if_exists": "run",
            },
        ]

        with patch_pipeline_client(mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        mock_client.uploads.get_by_id.assert_called_once_with("upload-1")
        assert result[0]["dataset_id"] == "id-1"
        assert result[1]["dataset_id"] == "id-2"

    def test_captures_errors_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []  # no INTENSITY dataset

        with patch_pipeline_client(mock_client):
            result = json.loads(
                run_dose_response_bulk(
                    [
                        {
                            "upload_id": "upload-bad",
                            "dataset_name": "DR A",
                            "sample_names": ["s1"],
                            "control_samples": ["s1"],
                        }
                    ]
                )
            )

        assert "error" in result[0]
        assert result[0]["error_code"] == "dataset_not_found"


class TestRunNormalisationImputationBulk:
    def test_submits_all_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [mock_initial_ds_dataset()]
        mock_client.datasets.create.side_effect = ["ni-id-1", "ni-id-2"]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": f"NI {i}",
                "normalisation_method": "median",
                "imputation_method": "mnar",
                "if_exists": "run",
            }
            for i in range(2)
        ]

        with patch_pipeline_client(mock_client):
            result = json.loads(run_normalisation_imputation_bulk(jobs))

        assert result[0]["dataset_id"] == "ni-id-1"
        assert result[1]["dataset_id"] == "ni-id-2"

    def test_skips_existing_jobs(self):
        ni_ds = MagicMock()
        ni_ds.id = "existing-ni-id"
        ni_ds.type = "NORMALISATION_IMPUTATION"
        ni_ds.name = "NI Run"
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [ni_ds]

        with patch_pipeline_client(mock_client):
            result = json.loads(
                run_normalisation_imputation_bulk(
                    [
                        {
                            "upload_id": "upload-1",
                            "dataset_name": "NI Run",
                            "normalisation_method": "median",
                            "imputation_method": "mnar",
                        }
                    ]
                )
            )

        assert result[0]["dataset_id"] == "existing-ni-id"
        assert result[0]["skipped"] is True
        mock_client.datasets.create.assert_not_called()

    def test_enforces_job_cap(self):
        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": f"NI {i}",
                "normalisation_method": "median",
                "imputation_method": "mnar",
            }
            for i in range(501)
        ]
        result = json.loads(run_normalisation_imputation_bulk(jobs))
        assert "error" in result
        assert "500" in result["error"]

    def test_captures_errors_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []  # no INTENSITY dataset

        with patch_pipeline_client(mock_client):
            result = json.loads(
                run_normalisation_imputation_bulk(
                    [
                        {
                            "upload_id": "upload-bad",
                            "dataset_name": "NI A",
                            "normalisation_method": "median",
                            "imputation_method": "mnar",
                        }
                    ]
                )
            )

        assert "error" in result[0]
        assert result[0]["error_code"] == "dataset_not_found"


class TestRunPairwiseComparisonBulk:
    _base_job = {
        "upload_id": "upload-1",
        "input_dataset_ids": ["435d321c-281e-4722-b08d-08f5b15de17f"],
        "dataset_name": "PC A",
        "sample_metadata": [
            ["sample_name", "condition"],
            ["s1", "ctrl"],
            ["s2", "treated"],
        ],
        "condition_column": "condition",
        "condition_comparisons": [["treated", "ctrl"]],
    }

    def test_submits_all_jobs(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.side_effect = ["pc-id-1", "pc-id-2"]

        jobs = [
            {**self._base_job, "dataset_name": f"PC {i}", "if_exists": "run"}
            for i in range(2)
        ]

        with patch_pipeline_client(mock_client):
            result = json.loads(run_pairwise_comparison_bulk(jobs))

        assert result[0]["dataset_id"] == "pc-id-1"
        assert result[1]["dataset_id"] == "pc-id-2"

    def test_skips_existing_jobs(self):
        pc_ds = MagicMock()
        pc_ds.id = "existing-pc-id"
        pc_ds.type = "PAIRWISE_COMPARISON"
        pc_ds.name = "PC A"
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [pc_ds]

        with patch_pipeline_client(mock_client):
            result = json.loads(run_pairwise_comparison_bulk([self._base_job]))

        assert result[0]["dataset_id"] == "existing-pc-id"
        assert result[0]["skipped"] is True
        mock_client.datasets.create.assert_not_called()

    def test_error_when_missing_input_dataset_ids(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []

        job = {k: v for k, v in self._base_job.items() if k != "input_dataset_ids"}
        job["if_exists"] = "run"

        with patch_pipeline_client(mock_client):
            result = json.loads(run_pairwise_comparison_bulk([job]))

        assert result[0]["error_code"] == "missing_input"

    def test_enforces_job_cap(self):
        jobs = [{**self._base_job, "dataset_name": f"PC {i}"} for i in range(501)]
        result = json.loads(run_pairwise_comparison_bulk(jobs))
        assert "error" in result

    def test_control_variables_not_double_nested(self):
        """control_variables should be passed as a raw list, not wrapped twice."""
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.create.return_value = "pc-id-1"

        cv = [{"variable": "batch", "values": [["s1", "A"], ["s2", "B"]]}]
        job = {**self._base_job, "if_exists": "run", "control_variables": cv}

        with patch_pipeline_client(mock_client):
            with patch(
                "mcp_tools.pipelines.pairwise.run_pairwise_comparison"
            ) as mock_run:
                mock_run.return_value = "Dataset ID: pc-id-1"
                run_pairwise_comparison_bulk([job])

        called_cv = mock_run.call_args.kwargs["control_variables"]
        assert called_cv == cv, f"Expected raw list, got: {called_cv}"
