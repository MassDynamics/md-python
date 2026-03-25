"""Tests for mcp_tools.health."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.health import _WORKFLOW_GUIDE, get_workflow_guide, health_check


class TestHealthCheck:
    def test_ok_response(self):
        mock_client = MagicMock()
        mock_client.health.check.return_value = {"status": "ok"}

        with patch("mcp_tools.health.get_client", return_value=mock_client):
            result = health_check()

        data = json.loads(result)
        assert data["status"] == "ok"

    def test_error_response(self):
        mock_client = MagicMock()
        mock_client.health.check.return_value = {
            "status": "error",
            "message": "unreachable",
        }

        with patch("mcp_tools.health.get_client", return_value=mock_client):
            result = health_check()

        data = json.loads(result)
        assert data["status"] == "error"
        assert data["message"] == "unreachable"


class TestWorkflowGuide:
    def test_returns_valid_json_with_expected_top_level_keys(self):
        data = json.loads(get_workflow_guide())
        assert isinstance(data, dict)
        for key in ("overview", "workflows", "tool_index", "constraints", "batch_tips"):
            assert key in data

    def test_all_four_workflows_present_with_description_and_steps(self):
        for name in (
            "A_upload_new_data",
            "B_full_DEA",
            "C_full_DRA",
            "D_format_conversion",
        ):
            wf = _WORKFLOW_GUIDE["workflows"][name]
            assert "description" in wf
            assert len(wf["steps"]) > 0

    def test_tool_index_covers_all_five_categories(self):
        index = _WORKFLOW_GUIDE["tool_index"]
        expected = {
            "file_tools",
            "upload_tools",
            "dataset_tools",
            "pipeline_tools",
            "utility_tools",
        }
        assert expected == set(index.keys())
        for category, tools in index.items():
            assert len(tools) > 0, f"category '{category}' is empty"

    def test_constraints_are_nonempty_strings(self):
        constraints = _WORKFLOW_GUIDE["constraints"]
        assert len(constraints) > 0
        assert all(isinstance(c, str) and c.strip() for c in constraints)
