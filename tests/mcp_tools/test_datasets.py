from unittest.mock import MagicMock, patch

from mcp_tools.datasets import (
    delete_dataset,
    find_initial_dataset,
    list_datasets,
    list_jobs,
    retry_dataset,
    wait_for_dataset,
)


def _mock_dataset(id="ds-1", name="My Dataset", type="INTENSITY", state="COMPLETED"):
    ds = MagicMock()
    ds.id = id
    ds.name = name
    ds.type = type
    ds.state = state
    ds.__str__ = lambda self: f"Dataset: {name}"
    return ds


def test_list_jobs():
    mock_client = MagicMock()
    mock_client.jobs.list.return_value = [
        {"slug": "normalisation_imputation", "name": "Normalisation & Imputation"},
        {"slug": "pairwise_comparison", "name": "Pairwise Comparison"},
    ]

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = list_jobs()

    assert "normalisation_imputation" in result
    assert "pairwise_comparison" in result


def test_list_jobs_empty():
    mock_client = MagicMock()
    mock_client.jobs.list.return_value = []

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = list_jobs()

    assert "No jobs" in result


def test_list_datasets_found():
    mock_client = MagicMock()
    mock_client.datasets.list_by_upload.return_value = [
        _mock_dataset("ds-1", "Initial", "INTENSITY", "COMPLETED"),
        _mock_dataset("ds-2", "Pairwise", "PAIRWISE_COMPARISON", "PROCESSING"),
    ]

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = list_datasets("upload-123")

    mock_client.datasets.list_by_upload.assert_called_once_with("upload-123")
    assert "2 dataset(s)" in result
    assert "ds-1" in result
    assert "INTENSITY" in result


def test_list_datasets_empty():
    mock_client = MagicMock()
    mock_client.datasets.list_by_upload.return_value = []

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = list_datasets("upload-123")

    assert "No datasets" in result


def test_find_initial_dataset_found():
    mock_client = MagicMock()
    mock_client.datasets.find_initial_dataset.return_value = _mock_dataset()

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = find_initial_dataset("upload-123")

    mock_client.datasets.find_initial_dataset.assert_called_once_with("upload-123")
    assert "ds-1" in result
    assert "Initial dataset found" in result


def test_find_initial_dataset_not_found():
    mock_client = MagicMock()
    mock_client.datasets.find_initial_dataset.return_value = None

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = find_initial_dataset("upload-123")

    assert "No initial" in result


def test_wait_for_dataset():
    mock_client = MagicMock()
    mock_client.datasets.wait_until_complete.return_value = _mock_dataset(
        state="COMPLETED"
    )

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = wait_for_dataset(
            "upload-123", "ds-1", poll_seconds=1, timeout_seconds=60
        )

    mock_client.datasets.wait_until_complete.assert_called_once_with(
        "upload-123", "ds-1", poll_s=1, timeout_s=60
    )
    assert "Dataset: My Dataset" in result


def test_retry_dataset_success():
    mock_client = MagicMock()
    mock_client.datasets.retry.return_value = True

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = retry_dataset("ds-1")

    assert "successfully" in result


def test_retry_dataset_failure():
    mock_client = MagicMock()
    mock_client.datasets.retry.return_value = False

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = retry_dataset("ds-1")

    assert "Failed" in result


def test_delete_dataset_success():
    mock_client = MagicMock()
    mock_client.datasets.delete.return_value = True

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = delete_dataset("ds-1")

    assert "successfully" in result


def test_delete_dataset_failure():
    mock_client = MagicMock()
    mock_client.datasets.delete.return_value = False

    with patch("mcp_tools.datasets.get_client", return_value=mock_client):
        result = delete_dataset("ds-1")

    assert "Failed" in result
