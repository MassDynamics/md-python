"""Tests for query_entities MCP tool."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.entities import query_entities


class TestQueryEntities:
    def test_returns_json_result(self):
        result = {
            "results": [
                {"gene_name": "BRCA1", "dataset_id": "abc123"},
                {"gene_name": "BRCA1", "dataset_id": "def456"},
            ]
        }
        mock_client = MagicMock()
        mock_client.entities.query.return_value = result
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(query_entities("BRCA1", ["abc123", "def456"]))
        assert output == result
        mock_client.entities.query.assert_called_once_with(
            keyword="BRCA1", dataset_ids=["abc123", "def456"]
        )

    def test_returns_empty_results(self):
        mock_client = MagicMock()
        mock_client.entities.query.return_value = {"results": []}
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(query_entities("NONEXISTENT", ["abc123"]))
        assert output == {"results": []}

    def test_returns_error_on_exception(self):
        mock_client = MagicMock()
        mock_client.entities.query.side_effect = Exception(
            "Failed to query entities: 502 - upstream"
        )
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(query_entities("BRCA1", ["abc123"]))
        assert "error" in output
        assert "502" in output["error"]
