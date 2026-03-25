"""Tests for search_entities MCP tool."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.entities import search_entities


class TestSearchEntities:
    def test_returns_json_results(self):
        results = [
            {
                "dataset_id": "abc123",
                "entity_type": "protein",
                "items": [{"ProteinIds": ["P12345"], "GeneNames": ["BRCA1"]}],
            }
        ]
        mock_client = MagicMock()
        mock_client.entities.search.return_value = results
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(search_entities("BRCA1", ["abc123"]))
        assert output == results
        mock_client.entities.search.assert_called_once_with(
            keyword="BRCA1", dataset_ids=["abc123"]
        )

    def test_returns_error_on_permission_error(self):
        mock_client = MagicMock()
        mock_client.entities.search.side_effect = PermissionError("not enabled")
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(search_entities("BRCA1", ["abc123"]))
        assert "error" in output
        assert output["code"] == 403

    def test_returns_error_on_value_error(self):
        mock_client = MagicMock()
        mock_client.entities.search.side_effect = ValueError("keyword too short")
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(search_entities("X", ["abc123"]))
        assert "error" in output
        assert output["code"] == 400

    def test_returns_error_on_generic_exception(self):
        mock_client = MagicMock()
        mock_client.entities.search.side_effect = Exception("502 upstream failure")
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(search_entities("BRCA1", ["abc123"]))
        assert "error" in output
        assert "502" in output["error"]
