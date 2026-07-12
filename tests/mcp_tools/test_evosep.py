"""Tests for the create_evosep_qc MCP tool.

Source-of-truth: workflow app/api/api/v2/evosep_qcs/create.rb
(POST /evosep_qcs, feature-flagged behind Flipper flag ``evosep_qc``,
returns 201 {id, filename, uploaded_by, created_at}).
"""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.batch import _TOOL_REGISTRY
from mcp_tools.evosep import create_evosep_qc


class TestCreateEvosepQc:
    def test_returns_json_result(self):
        result = {
            "id": "11111111-1111-1111-1111-111111111111",
            "filename": "qc.raw",
            "uploaded_by": "user@example.com",
            "created_at": "2026-07-12T00:00:00Z",
        }
        mock_client = MagicMock()
        mock_client.evosep_qcs.create.return_value = result
        with patch("mcp_tools.evosep.get_client", return_value=mock_client):
            output = json.loads(create_evosep_qc("qc.raw", {"metric": 1.23}))
        assert output == result
        mock_client.evosep_qcs.create.assert_called_once_with(
            filename="qc.raw", blob={"metric": 1.23}
        )

    def test_feature_flag_off_returns_error_envelope(self):
        mock_client = MagicMock()
        mock_client.evosep_qcs.create.side_effect = Exception(
            'Failed to create evosep_qc: 404 - {"error":"Not found"}'
        )
        with patch("mcp_tools.evosep.get_client", return_value=mock_client):
            output = json.loads(create_evosep_qc("qc.raw", {}))
        assert "error" in output
        assert "404" in output["error"]

    def test_server_error_returns_error_envelope(self):
        mock_client = MagicMock()
        mock_client.evosep_qcs.create.side_effect = Exception(
            "Failed to create evosep_qc: 500 - Internal error"
        )
        with patch("mcp_tools.evosep.get_client", return_value=mock_client):
            output = json.loads(create_evosep_qc("qc.raw", {"metric": 1.0}))
        assert "error" in output
        assert "500" in output["error"]


class TestCreateEvosepQcRegistration:
    def test_is_in_tool_registry(self):
        assert "create_evosep_qc" in _TOOL_REGISTRY
        assert _TOOL_REGISTRY["create_evosep_qc"] is create_evosep_qc
