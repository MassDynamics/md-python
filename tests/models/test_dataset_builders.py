from uuid import UUID

from md_python.models import SampleMetadata
from md_python.models.dataset_builders import (
    DoseResponseDataset,
    MinimalDataset,
    NormalisationImputationDataset,
    PairwiseComparisonDataset,
)


def test_dose_response_dataset_build_and_run(mocker):
    drc = DoseResponseDataset(
        input_dataset_ids=[str(UUID(int=4))],
        dataset_name="Test doseresponse dataset",
        sample_names=["1", "2", "3", "4", "5", "6"],
        control_samples=["1", "3"],
        log_intensities=True,
        use_imputed_intensities=True,
        normalise="none",
        span_rollmean_k=1,
        prop_required_in_protein=0.5,
    )
    ds = drc.to_dataset()
    assert ds.name == "Test doseresponse dataset"
    assert ds.job_slug == "dose_response"
    assert ds.sample_names == ["1", "2", "3", "4", "5", "6"]
    assert ds.job_run_params["control_samples"] == ["1", "3"]
    assert ds.job_run_params["log_intensities"] is True
    assert ds.job_run_params["normalise"] == "none"
    assert ds.job_run_params["prop_required_in_protein"] == 0.5

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "drc-id"
    out = drc.run(client)
    assert out == "drc-id"


def test_pairwise_comparison_dataset_class_build_and_run(mocker):
    sm = SampleMetadata(data=[["group"], ["a"], ["b"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="Pairwise",
        sample_metadata=sm,
        condition_column="group",
        condition_comparisons=[["a", "b"]],
        # filter_values_criteria={"method": "percentage", "filter_threshold_percentage": 0.5},
    )
    ds = pw.to_dataset()
    assert ds.name == "Pairwise"

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "new-id"
    out = pw.run(client)
    assert out == "new-id"


def test_minimal_dataset_build_and_run(mocker):
    md = MinimalDataset(
        input_dataset_ids=[str(UUID(int=2))],
        dataset_name="Min DS",
        job_slug="demo_flow",
    )
    ds = md.to_dataset()
    assert ds.name == "Min DS"
    assert ds.job_slug == "demo_flow"
    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "min-id"
    out = md.run(client)
    assert out == "min-id"


def test_builders_validation_errors():
    # MinimalDataset validation
    md = MinimalDataset(input_dataset_ids=[], dataset_name="", job_slug="")
    try:
        md.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert (
            "input_dataset_ids" in str(e)
            or "dataset_name" in str(e)
            or "job_slug" in str(e)
        )

    # PairwiseComparisonDataset validation
    sm = SampleMetadata(data=[["group"], ["a"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[],
        dataset_name="",
        sample_metadata=sm,
        condition_column="",
        condition_comparisons=[],
        filter_values_criteria={
            "method": "percentage",
            "filter_threshold_percentage": 0.5,
        },
    )
    try:
        pw.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert any(
            k in str(e)
            for k in [
                "input_dataset_ids",
                "dataset_name",
                "condition_column",
                "condition_comparisons",
            ]
        )

    # DoseResponseDataset validation: control_samples must be subset of sample_names
    drc = DoseResponseDataset(
        input_dataset_ids=[str(UUID(int=0))],
        dataset_name="DRC",
        sample_names=["a", "b"],
        control_samples=["c"],
    )
    try:
        drc.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "control_samples" in str(e) and "sample_names" in str(e)

    # NormalisationImputationDataset validation
    ni = NormalisationImputationDataset(
        input_dataset_ids=[],
        dataset_name="",
        normalisation_methods={},
        imputation_methods={},
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert any(k in str(e) for k in ["input_dataset_ids", "dataset_name", "method"])


def test_normalisation_imputation_builder_build_and_run(mocker):
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=3))],
        dataset_name="NI DS",
        normalisation_methods={"method": "quantile"},
        imputation_methods={"method": "mnar", "std_position": 1.8, "std_width": 0.3},
    )
    ds = ni.to_dataset()
    assert ds.name == "NI DS"
    assert ds.job_slug == "normalisation_imputation"
    assert ds.job_run_params["normalisation_methods"]["method"] == "quantile"
    assert ds.job_run_params["imputation_methods"]["method"] == "mnar"

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "new-id"
    out = ni.run(client)
    assert out == "new-id"
