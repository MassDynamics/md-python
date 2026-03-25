"""Tests for mcp_tools.batch."""

import json
from unittest.mock import patch

from mcp_tools.batch import batch


class TestBatch:
    def test_single_operation(self):
        with patch("mcp_tools.health.get_client") as mock_get_client:
            mock_get_client.return_value.health.check.return_value = {"status": "ok"}
            result = batch([{"tool": "health_check"}])
        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["tool"] == "health_check"
        assert "result" in data[0]

    def test_multiple_operations(self):
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
        assert "Upload: test" in data[1]["result"]

    def test_stops_on_error_by_default(self):
        with patch("mcp_tools.uploads.get_client") as mock_client:
            mock_client.return_value.uploads.get_by_id.side_effect = Exception(
                "not found"
            )
            result = batch(
                [
                    {"tool": "get_upload", "params": {"upload_id": "bad-id"}},
                    {"tool": "health_check"},
                ]
            )
        data = json.loads(result)
        assert len(data) == 1
        assert "error" in data[0]

    def test_continues_on_error_when_flag_false(self):
        with (
            patch("mcp_tools.uploads.get_client") as mock_uploads,
            patch("mcp_tools.health.get_client") as mock_health,
        ):
            mock_uploads.return_value.uploads.get_by_id.side_effect = Exception(
                "not found"
            )
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

    def test_get_upload_by_name(self):
        mock_upload = type(
            "U", (), {"__str__": lambda self: "Upload: my-exp | ID: abc-123"}
        )()
        with patch("mcp_tools.uploads.get_client") as mock_client:
            mock_client.return_value.uploads.get_by_name.return_value = mock_upload
            result = batch([{"tool": "get_upload", "params": {"name": "my-exp"}}])
        data = json.loads(result)
        assert "my-exp" in data[0]["result"]
        mock_client.return_value.uploads.get_by_name.assert_called_once_with("my-exp")

    def test_unknown_tool_returns_error(self):
        data = json.loads(batch([{"tool": "nonexistent_tool"}]))
        assert "error" in data[0]
        assert "Unknown tool" in data[0]["error"]

    def test_error_entry_has_error_code_unknown_tool(self):
        data = json.loads(batch([{"tool": "nonexistent_tool"}]))
        assert data[0]["error_code"] == "unknown_tool"

    def test_error_entry_has_error_code_exception(self):
        with patch("mcp_tools.uploads.get_client") as mock_client:
            mock_client.return_value.uploads.get_by_id.side_effect = Exception("boom")
            data = json.loads(
                batch([{"tool": "get_upload", "params": {"upload_id": "x"}}])
            )
        assert data[0]["error_code"] == "exception"

    def test_index_matches_position(self):
        with patch("mcp_tools.health.get_client") as mock_health:
            mock_health.return_value.health.check.return_value = {"status": "ok"}
            result = batch(
                [{"tool": "health_check"}, {"tool": "health_check"}],
                stop_on_error=False,
            )
        data = json.loads(result)
        assert data[0]["index"] == 0
        assert data[1]["index"] == 1

    def test_empty_operations_returns_empty_list(self):
        assert json.loads(batch([])) == []

    def test_invalid_params_captured_as_error(self):
        data = json.loads(
            batch([{"tool": "health_check", "params": {"bad_param": "x"}}])
        )
        assert "error" in data[0]
