from unittest.mock import MagicMock, patch

from mcp_tools.uploads import (
    create_upload,
    get_upload,
    update_sample_metadata,
    wait_for_upload,
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


def test_get_upload_by_id():
    mock_upload = MagicMock()
    mock_upload.__str__ = lambda self: "Upload: test"
    mock_client = MagicMock()
    mock_client.uploads.get_by_id.return_value = mock_upload

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = get_upload(upload_id="abc-123")

    mock_client.uploads.get_by_id.assert_called_once_with("abc-123")
    assert "Upload: test" in result


def test_get_upload_by_name():
    mock_upload = MagicMock()
    mock_upload.__str__ = lambda self: "Upload: my-upload"
    mock_client = MagicMock()
    mock_client.uploads.get_by_name.return_value = mock_upload

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = get_upload(name="my-upload")

    mock_client.uploads.get_by_name.assert_called_once_with("my-upload")
    assert "Upload: my-upload" in result


def test_get_upload_not_found():
    mock_client = MagicMock()
    mock_client.uploads.get_by_id.return_value = None

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = get_upload(upload_id="missing")

    assert "not found" in result.lower()


def test_get_upload_no_args():
    result = get_upload()
    assert "Error" in result


def test_create_upload_s3():
    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-001"

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = create_upload(
            name="Test Upload",
            source="diann_tabular",
            experiment_design=DESIGN,
            sample_metadata=METADATA,
            s3_bucket="my-bucket",
            s3_prefix="data/",
            filenames=["report.tsv"],
        )

    assert "upload-id-001" in result
    mock_client.uploads.create.assert_called_once()


def test_create_upload_local_files():
    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-002"

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = create_upload(
            name="Test Upload",
            source="diann_tabular",
            experiment_design=DESIGN,
            sample_metadata=METADATA,
            file_location="/data/files",
            filenames=["report.tsv"],
        )

    assert "upload-id-002" in result


def test_update_sample_metadata_success():
    mock_client = MagicMock()
    mock_client.uploads.update_sample_metadata.return_value = True

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = update_sample_metadata("upload-123", METADATA)

    assert "successfully" in result


def test_update_sample_metadata_failure():
    mock_client = MagicMock()
    mock_client.uploads.update_sample_metadata.return_value = False

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = update_sample_metadata("upload-123", METADATA)

    assert "Failed" in result


def test_wait_for_upload():
    mock_upload = MagicMock()
    mock_upload.__str__ = lambda self: "Upload: done | Status: COMPLETED"
    mock_client = MagicMock()
    mock_client.uploads.wait_until_complete.return_value = mock_upload

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = wait_for_upload("upload-123", poll_seconds=1, timeout_seconds=60)

    mock_client.uploads.wait_until_complete.assert_called_once_with(
        "upload-123", poll_s=1, timeout_s=60
    )
    assert "COMPLETED" in result
