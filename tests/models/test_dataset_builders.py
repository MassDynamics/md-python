from uuid import UUID

from md_python.models import SampleMetadata
from md_python.models.dataset_builders import PairwiseComparisonDataset, MinimalDataset



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
        assert "input_dataset_ids" in str(e) or "dataset_name" in str(e) or "job_slug" in str(e)

    # PairwiseComparisonDataset validation
    sm = SampleMetadata(data=[["group"], ["a"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[],
        dataset_name="",
        sample_metadata=sm,
        condition_column="",
        condition_comparisons=[],
        filter_values_criteria={"method": "percentage", "filter_threshold_percentage": 0.5},
    )
    try:
        pw.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert any(k in str(e) for k in ["input_dataset_ids", "dataset_name", "condition_column", "condition_comparisons"]) 

