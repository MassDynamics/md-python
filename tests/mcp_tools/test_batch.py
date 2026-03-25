import json
from unittest.mock import patch

from mcp_tools.batch import batch

DESIGN = [
    ["filename", "sample_name", "condition"],
    ["file1.raw", "S1", "ctrl"],
    ["file2.raw", "S2", "treated"],
]

METADATA = [
    ["sample_name", "dose"],
    ["S1", "0"],
    ["S2", "10"],
]


def test_batch_single_operation():
    with patch("mcp_tools.health.get_client") as mock_get_client:
        mock_get_client.return_value.health.check.return_value = {"status": "ok"}
        result = batch([{"tool": "health_check"}])

    data = json.loads(result)
    assert len(data) == 1
    assert data[0]["tool"] == "health_check"
    assert "result" in data[0]


def test_batch_multiple_operations():
    with (
        patch("mcp_tools.health.get_client") as mock_health,
        patch("mcp_tools.uploads.get_client") as mock_uploads,
    ):
        mock_health.return_value.health.check.return_value = {"status": "ok"}

        mock_upload = type("U", (), {"__str__": lambda self: "Upload: test"})()
        mock_uploads.return_value.uploads.get_by_id.return_value = mock_upload

        result = batch(
            [
                {"tool": "health_check"},
                {"tool": "get_upload", "params": {"upload_id": "abc-123"}},
            ]
        )

    data = json.loads(result)
    assert len(data) == 2
    assert data[0]["tool"] == "health_check"
    assert data[1]["tool"] == "get_upload"
    assert "Upload: test" in data[1]["result"]


def test_batch_stops_on_error_by_default():
    with patch("mcp_tools.uploads.get_client") as mock_client:
        mock_client.return_value.uploads.get_by_id.side_effect = Exception("not found")

        result = batch(
            [
                {"tool": "get_upload", "params": {"upload_id": "bad-id"}},
                {"tool": "health_check"},
            ]
        )

    data = json.loads(result)
    assert len(data) == 1
    assert "error" in data[0]


def test_batch_continues_on_error_when_flag_false():
    with (
        patch("mcp_tools.uploads.get_client") as mock_uploads,
        patch("mcp_tools.health.get_client") as mock_health,
    ):
        mock_uploads.return_value.uploads.get_by_id.side_effect = Exception("not found")
        mock_health.return_value.health.check.return_value = {"status": "ok"}

        result = batch(
            [
                {"tool": "get_upload", "params": {"upload_id": "bad-id"}},
                {"tool": "health_check"},
            ],
            stop_on_error=False,
        )

    data = json.loads(result)
    assert len(data) == 2
    assert "error" in data[0]
    assert "result" in data[1]


def test_batch_get_upload_by_name():
    """batch must forward the name param to get_upload (name-based lookup)."""
    mock_upload = type(
        "U", (), {"__str__": lambda self: "Upload: my-exp | ID: abc-123"}
    )()
    with patch("mcp_tools.uploads.get_client") as mock_client:
        mock_client.return_value.uploads.get_by_name.return_value = mock_upload

        result = batch([{"tool": "get_upload", "params": {"name": "my-exp"}}])

    data = json.loads(result)
    assert len(data) == 1
    assert data[0]["tool"] == "get_upload"
    assert "result" in data[0]
    assert "my-exp" in data[0]["result"]
    mock_client.return_value.uploads.get_by_name.assert_called_once_with("my-exp")


def test_batch_unknown_tool():
    result = batch([{"tool": "nonexistent_tool"}])
    data = json.loads(result)
    assert "error" in data[0]
    assert "Unknown tool" in data[0]["error"]


def test_batch_empty_operations():
    """An empty operations list returns an empty JSON array."""
    result = batch([])
    data = json.loads(result)
    assert data == []


def test_batch_invalid_params_captured_as_error():
    """Passing unexpected kwargs to a tool is caught and reported, not raised."""
    result = batch([{"tool": "health_check", "params": {"nonexistent_param": "value"}}])
    data = json.loads(result)
    assert "error" in data[0]


def test_batch_index_is_correct():
    with patch("mcp_tools.health.get_client") as mock_health:
        mock_health.return_value.health.check.return_value = {"status": "ok"}

        result = batch(
            [{"tool": "health_check"}, {"tool": "health_check"}],
            stop_on_error=False,
        )

    data = json.loads(result)
    assert data[0]["index"] == 0
    assert data[1]["index"] == 1
