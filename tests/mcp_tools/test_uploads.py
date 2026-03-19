from unittest.mock import MagicMock, patch

from mcp_tools.uploads import (
    create_upload,
    get_upload,
    update_sample_metadata,
    validate_upload_inputs,
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


def test_validate_upload_inputs_ok():
    result = validate_upload_inputs(DESIGN, METADATA)
    assert result.startswith("OK")
    assert "2 samples" in result


def test_validate_upload_inputs_missing_sample_in_metadata():
    bad_metadata = [
        ["sample_name", "dose"],
        ["s1", "0"],
        # s2 missing
    ]
    result = validate_upload_inputs(DESIGN, bad_metadata)
    assert "Validation failed" in result
    assert "s2" in result


def test_validate_upload_inputs_extra_sample_in_metadata():
    extra_metadata = [
        ["sample_name", "dose"],
        ["s1", "0"],
        ["s2", "10"],
        ["s3", "20"],  # not in design
    ]
    result = validate_upload_inputs(DESIGN, extra_metadata)
    assert "Validation failed" in result
    assert "s3" in result


def test_validate_upload_inputs_missing_column_in_design():
    bad_design = [
        ["filename", "sample_name"],  # missing condition
        ["file1.tsv", "s1"],
    ]
    result = validate_upload_inputs(bad_design, METADATA)
    assert "missing required column" in result


def test_validate_upload_inputs_no_sample_name_in_metadata():
    bad_metadata = [
        ["name", "dose"],  # 'name' is not 'sample_name'
        ["s1", "0"],
    ]
    result = validate_upload_inputs(bad_metadata, DESIGN)
    # Called with args swapped — should flag missing sample_name col
    result = validate_upload_inputs(DESIGN, bad_metadata)
    assert "sample_name" in result


def test_validate_upload_inputs_synonym_columns():
    design_synonyms = [
        ["file", "sample", "group"],
        ["file1.tsv", "s1", "ctrl"],
        ["file2.tsv", "s2", "treated"],
    ]
    result = validate_upload_inputs(design_synonyms, METADATA)
    assert result.startswith("OK")


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
