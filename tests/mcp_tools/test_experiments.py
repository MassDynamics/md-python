from unittest.mock import MagicMock, patch

import pytest

from mcp_tools.experiments import (
    create_experiment,
    get_experiment,
    update_sample_metadata,
    wait_for_experiment,
)

DESIGN = [
    ["filename", "sample_name", "condition"],
    ["file1.tsv", "s1", "ctrl"],
    ["file2.tsv", "s2", "treated"],
]

METADATA = [
    ["sample_name", "dose"],
    ["s1", "0"],
    ["s2", "10"],
]


def test_get_experiment_by_id():
    mock_exp = MagicMock()
    mock_exp.__str__ = lambda self: "Experiment: test"
    mock_client = MagicMock()
    mock_client.experiments.get_by_id.return_value = mock_exp

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = get_experiment(experiment_id="abc-123")

    mock_client.experiments.get_by_id.assert_called_once_with("abc-123")
    assert "Experiment: test" in result


def test_get_experiment_by_name():
    mock_exp = MagicMock()
    mock_exp.__str__ = lambda self: "Experiment: my-exp"
    mock_client = MagicMock()
    mock_client.experiments.get_by_name.return_value = mock_exp

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = get_experiment(name="my-exp")

    mock_client.experiments.get_by_name.assert_called_once_with("my-exp")
    assert "Experiment: my-exp" in result


def test_get_experiment_not_found():
    mock_client = MagicMock()
    mock_client.experiments.get_by_id.return_value = None

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = get_experiment(experiment_id="missing")

    assert "not found" in result.lower()


def test_get_experiment_no_args():
    result = get_experiment()
    assert "Error" in result


def test_create_experiment_s3():
    mock_client = MagicMock()
    mock_client.experiments.create.return_value = "exp-id-001"

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = create_experiment(
            name="Test Exp",
            source="diann_tabular",
            experiment_design=DESIGN,
            s3_bucket="my-bucket",
            s3_prefix="data/",
            filenames=["report.tsv"],
        )

    assert "exp-id-001" in result
    mock_client.experiments.create.assert_called_once()


def test_create_experiment_with_sample_metadata():
    mock_client = MagicMock()
    mock_client.experiments.create.return_value = "exp-id-002"

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = create_experiment(
            name="Test Exp",
            source="diann_tabular",
            experiment_design=DESIGN,
            sample_metadata=METADATA,
            s3_bucket="my-bucket",
            s3_prefix="data/",
            filenames=["report.tsv"],
        )

    assert "exp-id-002" in result


def test_update_sample_metadata_success():
    mock_client = MagicMock()
    mock_client.experiments.update_sample_metadata.return_value = True

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = update_sample_metadata("exp-123", METADATA)

    assert "successfully" in result


def test_update_sample_metadata_failure():
    mock_client = MagicMock()
    mock_client.experiments.update_sample_metadata.return_value = False

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = update_sample_metadata("exp-123", METADATA)

    assert "Failed" in result


def test_wait_for_experiment():
    mock_exp = MagicMock()
    mock_exp.__str__ = lambda self: "Experiment: done | Status: COMPLETED"
    mock_client = MagicMock()
    mock_client.experiments.wait_until_complete.return_value = mock_exp

    with patch("mcp_tools.experiments.get_client", return_value=mock_client):
        result = wait_for_experiment("exp-123", poll_seconds=1, timeout_seconds=60)

    mock_client.experiments.wait_until_complete.assert_called_once_with(
        "exp-123", poll_s=1, timeout_s=60
    )
    assert "COMPLETED" in result
