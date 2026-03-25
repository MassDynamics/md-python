import json
import os
import tempfile
from unittest.mock import MagicMock, patch

from mcp_tools.uploads import (
    cancel_upload_queue,
    create_upload,
    create_upload_from_csv,
    get_upload,
    list_uploads_status,
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


def test_wait_for_upload_timeout_returns_current_status():
    mock_upload = MagicMock()
    mock_upload.status = "PROCESSING"
    mock_upload.__str__ = lambda self: "Upload: processing"
    mock_client = MagicMock()
    mock_client.uploads.wait_until_complete.side_effect = TimeoutError("timed out")
    mock_client.uploads.get_by_id.return_value = mock_upload

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = wait_for_upload("upload-123", poll_seconds=1, timeout_seconds=5)

    assert "PROCESSING" in result
    assert "call wait_for_upload again" in result


def test_create_upload_auto_discovers_filenames(tmp_path):
    (tmp_path / "report.tsv").write_text("data")
    (tmp_path / "metadata.csv").write_text("data")
    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-003"

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = create_upload(
            name="Test",
            source="diann_tabular",
            experiment_design=DESIGN,
            sample_metadata=METADATA,
            file_location=str(tmp_path),
            # filenames omitted — should be auto-discovered
        )

    assert "upload-id-003" in result
    call_args = mock_client.uploads.create.call_args[0][0]
    assert sorted(call_args.filenames) == sorted(["metadata.csv", "report.tsv"])


def test_create_upload_from_csv_returns_upload_id(tmp_path):
    # Write a valid combined CSV
    csv = tmp_path / "metadata.csv"
    csv.write_text("filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n")
    (tmp_path / "file1.tsv").write_text("data")
    (tmp_path / "file2.tsv").write_text("data")

    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-from-csv"

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = create_upload_from_csv(
            name="CSV Upload",
            source="md_format",
            metadata_csv_path=str(csv),
            file_location=str(tmp_path),
        )

    assert "upload-id-from-csv" in result
    # background=True must be passed to client.uploads.create
    mock_client.uploads.create.assert_called_once()
    _, kwargs = mock_client.uploads.create.call_args
    assert (
        kwargs.get("background") is True
        or mock_client.uploads.create.call_args[1].get("background") is True
        or mock_client.uploads.create.call_args[0][1] is True
    )


def test_create_upload_from_csv_bad_csv(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("sample_name,dose\ns1,0\ns2,10\n")  # no filename/condition cols

    with patch("mcp_tools.uploads.get_client", return_value=MagicMock()):
        result = create_upload_from_csv(
            name="Bad",
            source="md_format",
            metadata_csv_path=str(csv),
            file_location=str(tmp_path),
        )

    assert "Error" in result


def test_create_upload_from_csv_large_files_uses_sequential_executor(tmp_path):
    """Files above threshold are routed through the module-level sequential executor."""
    from mcp_tools.uploads import _large_upload_executor

    csv = tmp_path / "metadata.csv"
    csv.write_text("filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n")
    (tmp_path / "file1.tsv").write_text("data")
    (tmp_path / "file2.tsv").write_text("data")

    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-large"

    # 60 MB per file × 2 = 120 MB > 100 MB threshold
    large_size = 60 * 1024 * 1024
    with (
        patch("mcp_tools.uploads.get_client", return_value=mock_client),
        patch("mcp_tools.uploads.os.path.getsize", return_value=large_size),
    ):
        result = create_upload_from_csv(
            name="Large Upload",
            source="md_format",
            metadata_csv_path=str(csv),
            file_location=str(tmp_path),
        )

    assert "upload-id-large" in result
    assert "queued" in result
    _, kwargs = mock_client.uploads.create.call_args
    assert kwargs.get("background") is True
    assert kwargs.get("executor") is _large_upload_executor


def test_create_upload_from_csv_small_files_no_executor(tmp_path):
    """Files below threshold use a plain background thread (executor=None)."""
    csv = tmp_path / "metadata.csv"
    csv.write_text("filename,sample_name,condition\nfile1,s1,ctrl\nfile2,s2,treated\n")
    (tmp_path / "file1.tsv").write_text("data")
    (tmp_path / "file2.tsv").write_text("data")

    mock_client = MagicMock()
    mock_client.uploads.create.return_value = "upload-id-small"

    # 10 MB per file × 2 = 20 MB < 100 MB threshold
    small_size = 10 * 1024 * 1024
    with (
        patch("mcp_tools.uploads.get_client", return_value=mock_client),
        patch("mcp_tools.uploads.os.path.getsize", return_value=small_size),
    ):
        result = create_upload_from_csv(
            name="Small Upload",
            source="md_format",
            metadata_csv_path=str(csv),
            file_location=str(tmp_path),
        )

    assert "upload-id-small" in result
    assert "queued" not in result
    _, kwargs = mock_client.uploads.create.call_args
    assert kwargs.get("background") is True
    assert kwargs.get("executor") is None


def test_cancel_upload_queue_resets_executor():
    import mcp_tools.uploads as uploads_module

    original_executor = uploads_module._large_upload_executor
    # Patch via the accessor so we don't mutate the live variable directly
    with patch("mcp_tools.uploads._get_executor", return_value=original_executor):
        with patch.object(original_executor, "shutdown") as mock_shutdown:
            result = cancel_upload_queue()

    mock_shutdown.assert_called_once_with(wait=False, cancel_futures=True)
    assert uploads_module._large_upload_executor is not original_executor
    assert "reset" in result.lower()


def test_validate_upload_inputs_empty_design():
    result = validate_upload_inputs([], METADATA)
    assert "Error" in result
    assert "experiment_design" in result


def test_validate_upload_inputs_empty_metadata():
    result = validate_upload_inputs(DESIGN, [])
    assert "Error" in result
    assert "sample_metadata" in result


def test_validate_upload_inputs_duplicate_sample_in_design():
    dupe_design = [
        ["filename", "sample_name", "condition"],
        ["file1.tsv", "s1", "ctrl"],
        ["file2.tsv", "s1", "ctrl"],  # duplicate s1
    ]
    result = validate_upload_inputs(dupe_design, METADATA)
    assert "Duplicate" in result
    assert "s1" in result


def test_create_upload_from_csv_missing_file_location(tmp_path):
    csv = tmp_path / "metadata.csv"
    csv.write_text("filename,sample_name,condition\nfile1,s1,ctrl\n")

    with patch("mcp_tools.uploads.get_client", return_value=MagicMock()):
        result = create_upload_from_csv(
            name="Test",
            source="md_format",
            metadata_csv_path=str(csv),
            file_location="/nonexistent/path",
        )

    assert "Error" in result
    assert "file_location" in result


def test_list_uploads_status():
    mock_u1 = MagicMock()
    mock_u1.name = "exp1"
    mock_u1.status = "COMPLETED"
    mock_u1.source = "diann_tabular"

    mock_u2 = MagicMock()
    mock_u2.name = "exp2"
    mock_u2.status = "PROCESSING"
    mock_u2.source = "maxquant"

    mock_client = MagicMock()
    mock_client.uploads.get_by_id.side_effect = [mock_u1, mock_u2]

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = list_uploads_status(["uid-1", "uid-2"])

    data = json.loads(result)
    assert data["uid-1"]["status"] == "COMPLETED"
    assert data["uid-2"]["status"] == "PROCESSING"


def test_list_uploads_status_handles_errors():
    mock_u1 = MagicMock()
    mock_u1.name = "exp1"
    mock_u1.status = "COMPLETED"
    mock_u1.source = "diann_tabular"

    mock_client = MagicMock()
    mock_client.uploads.get_by_id.side_effect = [mock_u1, Exception("not found")]

    with patch("mcp_tools.uploads.get_client", return_value=mock_client):
        result = list_uploads_status(["uid-ok", "uid-bad"])

    data = json.loads(result)
    assert data["uid-ok"]["status"] == "COMPLETED"
    assert "error" in data["uid-bad"]
