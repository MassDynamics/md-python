from uuid import UUID

from md_python.models import SampleMetadata
from md_python.utils.builders import PairwiseComparisonDataset, MinimalDataset



def test_pairwise_comparison_dataset_class_build_and_run(mocker):
    sm = SampleMetadata(data=[["group"], ["a"], ["b"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="Pairwise",
        sample_metadata=sm,
        condition_column="group",
        condition_comparisons=[["a", "b"]],
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

