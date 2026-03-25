import json
from unittest.mock import MagicMock, patch

from mcp_tools.health import _WORKFLOW_GUIDE, get_workflow_guide, health_check


def test_health_check_ok():
    mock_client = MagicMock()
    mock_client.health.check.return_value = {"status": "ok"}

    with patch("mcp_tools.health.get_client", return_value=mock_client):
        result = health_check()

    data = json.loads(result)
    assert data["status"] == "ok"


def test_health_check_error():
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


# ── Workflow guide ────────────────────────────────────────────────────────────


def test_workflow_guide_is_valid_json():
    result = get_workflow_guide()
    data = json.loads(result)
    assert isinstance(data, dict)
    assert "overview" in data
    assert "workflows" in data
    assert "tool_index" in data
    assert "constraints" in data
    assert "batch_tips" in data


def test_workflow_guide_has_all_workflows():
    data = _WORKFLOW_GUIDE
    workflows = data["workflows"]
    assert "A_upload_new_data" in workflows
    assert "B_full_DEA" in workflows
    assert "C_full_DRA" in workflows
    assert "D_format_conversion" in workflows
    for key, wf in workflows.items():
        assert "description" in wf, f"workflow {key} missing 'description'"
        assert "steps" in wf, f"workflow {key} missing 'steps'"
        assert len(wf["steps"]) > 0, f"workflow {key} has empty steps"


def test_workflow_guide_tool_index_complete():
    """Every tool category in the index must have at least one entry."""
    index = _WORKFLOW_GUIDE["tool_index"]
    expected_categories = {
        "file_tools",
        "upload_tools",
        "dataset_tools",
        "pipeline_tools",
        "utility_tools",
    }
    assert expected_categories == set(index.keys())
    for category, tools in index.items():
        assert len(tools) > 0, f"tool_index category '{category}' is empty"


def test_workflow_guide_constraints_nonempty():
    constraints = _WORKFLOW_GUIDE["constraints"]
    assert isinstance(constraints, list)
    assert len(constraints) > 0
    # All constraints should be non-empty strings
    assert all(isinstance(c, str) and c.strip() for c in constraints)
