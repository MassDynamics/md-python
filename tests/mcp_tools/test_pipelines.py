import json
from unittest.mock import MagicMock, call, patch

from mcp_tools.pipelines import (
    _filter_sample_metadata,
    describe_pipeline,
    generate_pairwise_comparisons,
    run_dose_response,
    run_dose_response_bulk,
    run_dose_response_from_upload,
    run_normalisation_imputation,
    run_pairwise_comparison,
)

INTENSITY_ID = "435d321c-281e-4722-b08d-08f5b15de17f"
OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"

SAMPLE_METADATA = [
    ["sample_name", "condition", "dose"],
    ["s1", "ctrl", "0"],
    ["s2", "ctrl", "0"],
    ["s3", "treated", "10"],
    ["s4", "treated", "10"],
]


class TestDescribePipeline:
    def test_known_slug_returns_json(self):
        result = json.loads(describe_pipeline("dose_response"))
        assert "parameters" in result
        assert "normalise" in result["parameters"]
        assert result["parameters"]["normalise"]["valid_values"] == [
            "none",
            "sum",
            "median",
        ]

    def test_all_slugs_have_required_and_parameters(self):
        for slug in (
            "normalisation_imputation",
            "dose_response",
            "pairwise_comparison",
        ):
            result = json.loads(describe_pipeline(slug))
            assert "required" in result, f"missing 'required' for {slug}"
            assert "parameters" in result, f"missing 'parameters' for {slug}"

    def test_normalisation_imputation_valid_methods(self):
        result = json.loads(describe_pipeline("normalisation_imputation"))
        norm_vals = result["parameters"]["normalisation_method"]["valid_values"]
        imp_vals = result["parameters"]["imputation_method"]["valid_values"]
        assert "median" in norm_vals
        assert "quantile" in norm_vals
        assert "min_value" in imp_vals
        assert "knn" in imp_vals

    def test_unknown_slug_returns_error(self):
        result = describe_pipeline("nonexistent_job")
        assert "Unknown job_slug" in result
        assert "nonexistent_job" in result


def test_run_normalisation_imputation():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_normalisation_imputation(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Norm",
            normalisation_method="median",
            imputation_method="min_value",
        )

    assert OUTPUT_ID in result
    mock_client.datasets.create.assert_called_once()


def test_run_normalisation_imputation_with_extra_params():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_normalisation_imputation(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Norm",
            normalisation_method="quantile",
            imputation_method="knn",
            normalisation_extra_params={"reference": "global"},
            imputation_extra_params={"k": 5},
        )

    assert OUTPUT_ID in result
    call_args = mock_client.datasets.create.call_args[0][0]
    assert call_args.job_run_params["normalisation_methods"]["reference"] == "global"
    assert call_args.job_run_params["imputation_methods"]["k"] == 5


def test_generate_pairwise_comparisons_vs_control():
    result = generate_pairwise_comparisons(
        sample_metadata=SAMPLE_METADATA,
        condition_column="condition",
        control="ctrl",
    )
    pairs = json.loads(result)
    assert ["treated", "ctrl"] in pairs
    assert all(p[1] == "ctrl" for p in pairs)


def test_generate_all_pairwise_comparisons():
    result = generate_pairwise_comparisons(
        sample_metadata=SAMPLE_METADATA,
        condition_column="condition",
    )
    pairs = json.loads(result)
    assert len(pairs) == 1
    assert ["treated", "ctrl"] in pairs


def test_run_pairwise_comparison():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_pairwise_comparison(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Pairwise",
            sample_metadata=SAMPLE_METADATA,
            condition_column="condition",
            condition_comparisons=[["treated", "ctrl"]],
        )

    assert OUTPUT_ID in result
    mock_client.datasets.create.assert_called_once()


def test_run_pairwise_comparison_with_control_variables():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_pairwise_comparison(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Pairwise",
            sample_metadata=SAMPLE_METADATA,
            condition_column="condition",
            condition_comparisons=[["treated", "ctrl"]],
            control_variables=[{"column": "dose", "type": "numerical"}],
        )

    assert OUTPUT_ID in result
    call_args = mock_client.datasets.create.call_args[0][0]
    cv = call_args.job_run_params["control_variables"]
    assert cv == {"control_variables": [{"column": "dose", "type": "numerical"}]}


def test_run_dose_response():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_dose_response(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Dose Response",
            sample_names=["s1", "s2", "s3", "s4"],
            control_samples=["s1", "s2"],
        )

    assert OUTPUT_ID in result
    mock_client.datasets.create.assert_called_once()


def test_run_dose_response_with_metadata():
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_dose_response(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="My Dose Response",
            sample_names=["s1", "s2", "s3", "s4"],
            control_samples=["s1", "s2"],
            sample_metadata=SAMPLE_METADATA,
            dose_column="dose",
        )

    assert OUTPUT_ID in result
    call_args = mock_client.datasets.create.call_args[0][0]
    assert "experiment_design" in call_args.job_run_params


def test_generate_pairwise_comparisons_single_condition():
    """With only one condition, no pairs can be formed — result is an empty list."""
    one_condition = [
        ["sample_name", "condition"],
        ["s1", "ctrl"],
        ["s2", "ctrl"],
    ]
    result = generate_pairwise_comparisons(
        sample_metadata=one_condition,
        condition_column="condition",
    )
    pairs = json.loads(result)
    assert pairs == []


def test_run_pairwise_comparison_result_contains_dataset_id():
    """The returned string must contain the dataset ID."""
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_pairwise_comparison(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="Test",
            sample_metadata=SAMPLE_METADATA,
            condition_column="condition",
            condition_comparisons=[["treated", "ctrl"]],
        )

    assert OUTPUT_ID in result


def test_run_dose_response_result_contains_dataset_id():
    """The returned string must contain the dataset ID."""
    mock_client = MagicMock()
    mock_client.datasets.create.return_value = OUTPUT_ID

    with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
        result = run_dose_response(
            input_dataset_ids=[INTENSITY_ID],
            dataset_name="Test DR",
            sample_names=["s1", "s2", "s3"],
            control_samples=["s1"],
        )

    assert OUTPUT_ID in result


# ---------------------------------------------------------------------------
# _filter_sample_metadata
# ---------------------------------------------------------------------------


class TestFilterSampleMetadata:
    FULL_META = [
        ["sample_name", "dose", "batch"],
        ["s1", "0", "A"],
        ["s2", "0", "A"],
        ["s3", "10", "B"],
        ["s4", "10", "B"],
    ]

    def test_filters_to_requested_samples(self):
        result = _filter_sample_metadata(self.FULL_META, ["s1", "s3"])
        assert result[0] == ["sample_name", "dose", "batch"]
        sample_names = [row[0] for row in result[1:]]
        assert sample_names == ["s1", "s3"]

    def test_preserves_header(self):
        result = _filter_sample_metadata(self.FULL_META, ["s2"])
        assert result[0] == self.FULL_META[0]

    def test_returns_all_rows_when_all_match(self):
        result = _filter_sample_metadata(self.FULL_META, ["s1", "s2", "s3", "s4"])
        assert len(result) == 5  # header + 4 rows

    def test_missing_sample_name_column_returns_unfiltered(self):
        no_sn = [["filename", "dose"], ["f1.raw", "0"]]
        result = _filter_sample_metadata(no_sn, ["s1"])
        assert result == no_sn

    def test_empty_metadata_returns_empty(self):
        assert _filter_sample_metadata([], ["s1"]) == []


# ---------------------------------------------------------------------------
# run_dose_response_from_upload
# ---------------------------------------------------------------------------


def _mock_initial_ds(dataset_id: str = INTENSITY_ID):
    ds = MagicMock()
    ds.id = dataset_id
    return ds


def _mock_dr_ds(
    dataset_id: str = OUTPUT_ID, name: str = "My DR", state: str = "COMPLETED"
):
    ds = MagicMock()
    ds.id = dataset_id
    ds.name = name
    ds.type = "DOSE_RESPONSE"
    ds.state = state
    return ds


class TestRunDoseResponseFromUpload:
    def test_finds_dataset_and_runs(self):
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = _mock_initial_ds()
        mock_client.datasets.list_by_upload.return_value = []  # no existing job
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2", "s3"],
                control_samples=["s1"],
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_skips_existing_job(self):
        mock_client = MagicMock()
        existing = _mock_dr_ds(dataset_id="existing-dr-id", name="My DR")
        mock_client.datasets.list_by_upload.return_value = [existing]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2"],
                control_samples=["s1"],
                if_exists="skip",
            )

        assert "existing-dr-id" in result
        assert "skipped" in result.lower()
        mock_client.datasets.create.assert_not_called()

    def test_runs_when_if_exists_is_run(self):
        mock_client = MagicMock()
        existing = _mock_dr_ds(name="My DR")
        mock_client.datasets.list_by_upload.return_value = [existing]
        mock_client.datasets.find_initial_dataset.return_value = _mock_initial_ds()
        mock_client.datasets.create.return_value = OUTPUT_ID

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s2"],
                control_samples=["s1"],
                if_exists="run",
            )

        assert OUTPUT_ID in result
        mock_client.datasets.create.assert_called_once()

    def test_auto_fetches_metadata_from_upload(self):
        """When sample_metadata is omitted, it should be fetched from the upload."""
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.return_value = _mock_initial_ds()
        mock_client.datasets.create.return_value = OUTPUT_ID

        full_meta = [
            ["sample_name", "dose"],
            ["s1", "0"],
            ["s2", "0"],
            ["s3", "10"],
        ]
        mock_upload = MagicMock()
        mock_upload.sample_metadata.data = full_meta
        mock_client.uploads.get_by_id.return_value = mock_upload

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-1",
                dataset_name="My DR",
                sample_names=["s1", "s3"],
                control_samples=["s1"],
                # sample_metadata intentionally omitted
            )

        assert OUTPUT_ID in result
        call_args = mock_client.datasets.create.call_args[0][0]
        # experiment_design is serialised as {column_name: [values...]}
        passed_meta = call_args.job_run_params["experiment_design"]
        sample_names_in_meta = passed_meta["sample_name"]
        assert "s1" in sample_names_in_meta
        assert "s3" in sample_names_in_meta
        assert "s2" not in sample_names_in_meta

    def test_no_initial_dataset_returns_error(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.return_value = None

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = run_dose_response_from_upload(
                upload_id="upload-missing",
                dataset_name="My DR",
                sample_names=["s1"],
                control_samples=["s1"],
            )

        assert "Error" in result
        assert "upload-missing" in result


class TestRunDoseResponseBulk:
    def test_runs_all_jobs(self):
        mock_client = MagicMock()
        mock_ds = _mock_initial_ds()
        mock_client.datasets.find_initial_dataset.return_value = mock_ds
        mock_client.datasets.list_by_upload.return_value = []  # no existing jobs
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

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        assert result[0]["dataset_id"] == "dr-id-1"
        assert result[1]["dataset_id"] == "dr-id-2"

    def test_skips_existing_jobs(self):
        existing = _mock_dr_ds(dataset_id="existing-id", name="DR A")
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = [existing]

        jobs = [
            {
                "upload_id": "upload-1",
                "dataset_name": "DR A",
                "sample_names": ["s1"],
                "control_samples": ["s1"],
            }
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        assert result[0]["dataset_id"] == "existing-id"
        assert result[0]["skipped"] is True
        mock_client.datasets.create.assert_not_called()

    def test_caches_dataset_lookup(self):
        """find_initial_dataset should be called once per unique upload_id."""
        mock_ds = _mock_initial_ds()
        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_ds
        mock_client.datasets.list_by_upload.return_value = []
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

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            run_dose_response_bulk(jobs)

        mock_client.datasets.find_initial_dataset.assert_called_once_with("upload-1")

    def test_auto_fetches_and_caches_upload_metadata(self):
        """Upload metadata should be fetched once per upload_id and filtered per job."""
        mock_ds = _mock_initial_ds()
        full_meta = [
            ["sample_name", "dose"],
            ["s1", "0"],
            ["s2", "10"],
            ["s3", "20"],
        ]
        mock_upload = MagicMock()
        mock_upload.sample_metadata.data = full_meta

        mock_client = MagicMock()
        mock_client.datasets.find_initial_dataset.return_value = mock_ds
        mock_client.datasets.list_by_upload.return_value = []
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

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        # Upload metadata fetched only once despite two jobs
        mock_client.uploads.get_by_id.assert_called_once_with("upload-1")
        assert result[0]["dataset_id"] == "id-1"
        assert result[1]["dataset_id"] == "id-2"

    def test_captures_errors_inline(self):
        mock_client = MagicMock()
        mock_client.datasets.list_by_upload.return_value = []
        mock_client.datasets.find_initial_dataset.side_effect = Exception("HTTP 404")

        jobs = [
            {
                "upload_id": "upload-bad",
                "dataset_name": "DR A",
                "sample_names": ["s1"],
                "control_samples": ["s1"],
            }
        ]

        with patch("mcp_tools.pipelines.get_client", return_value=mock_client):
            result = json.loads(run_dose_response_bulk(jobs))

        assert "error" in result[0]
        assert result[0]["error_code"] == "dataset_not_found"
