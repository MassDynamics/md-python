import json
from unittest.mock import MagicMock, patch

from mcp_tools.pipelines import (
    describe_pipeline,
    generate_pairwise_comparisons,
    run_dose_response,
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
