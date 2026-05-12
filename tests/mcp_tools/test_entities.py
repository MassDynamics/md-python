"""Tests for query_entities + map_protein_to_protein MCP tools."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.batch import _TOOL_REGISTRY
from mcp_tools.entities import map_protein_to_protein, query_entities
from mcp_tools.health import _WORKFLOW_GUIDE


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


class TestMapProteinToProtein:
    """Wraps client.entities.mappings.protein_to_protein.

    Source-of-truth: workflow/app/api/api/v2/entities/map/protein_to_protein.rb
    (POST /entities/mappings/protein_to_protein, returns {nodes, edges}).
    """

    def test_returns_nodes_and_edges_json(self):
        result = {
            "nodes": [
                {"id": "PG1", "type": "protein_group"},
                {"id": "PG2", "type": "protein_group"},
            ],
            "edges": [{"source": "PG1", "target": "PG2", "shared": ["P12345"]}],
        }
        mock_client = MagicMock()
        mock_client.entities.mappings.protein_to_protein.return_value = result
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(map_protein_to_protein(["abc123"], ["PG1"]))
        assert output == result
        mock_client.entities.mappings.protein_to_protein.assert_called_once_with(
            dataset_ids=["abc123"], entity_ids=["PG1"]
        )

    def test_returns_empty_graph(self):
        mock_client = MagicMock()
        mock_client.entities.mappings.protein_to_protein.return_value = {
            "nodes": [],
            "edges": [],
        }
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(map_protein_to_protein(["abc123"], ["NEVER_FOUND"]))
        assert output == {"nodes": [], "edges": []}

    def test_returns_error_on_exception(self):
        mock_client = MagicMock()
        mock_client.entities.mappings.protein_to_protein.side_effect = Exception(
            "Failed to map protein_to_protein: 403 - forbidden"
        )
        with patch("mcp_tools.entities.get_client", return_value=mock_client):
            output = json.loads(map_protein_to_protein(["abc123"], ["PG1"]))
        assert "error" in output
        assert "403" in output["error"]


class TestMapProteinToProteinRegistration:
    """Lock that the new tool is wired everywhere it needs to be."""

    def test_is_in_tool_registry(self):
        assert "map_protein_to_protein" in _TOOL_REGISTRY
        assert _TOOL_REGISTRY["map_protein_to_protein"] is map_protein_to_protein

    def test_is_in_workflow_guide_dataset_tools_index(self):
        index = _WORKFLOW_GUIDE["tool_index"]["dataset_tools"]
        assert "map_protein_to_protein" in index
        # Description should mention the {nodes, edges} return shape.
        desc = index["map_protein_to_protein"]
        assert "nodes" in desc and "edges" in desc

    def test_is_referenced_in_j_entity_lookup_workflow(self):
        steps = " ".join(_WORKFLOW_GUIDE["workflows"]["J_entity_lookup"]["steps"])
        assert "map_protein_to_protein" in steps
