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

    def test_tool_index_covers_all_categories(self):
        index = _WORKFLOW_GUIDE["tool_index"]
        expected = {
            "file_tools",
            "upload_tools",
            "dataset_tools",
            "pipeline_tools",
            "visualise_tools",
            "utility_tools",
        }
        assert expected == set(index.keys())
        for category, tools in index.items():
            assert len(tools) > 0, f"category '{category}' is empty"

    def test_constraints_are_nonempty_strings(self):
        constraints = _WORKFLOW_GUIDE["constraints"]
        assert len(constraints) > 0
        assert all(isinstance(c, str) and c.strip() for c in constraints)

    def test_filtration_only_workflow_present(self):
        wf = _WORKFLOW_GUIDE["workflows"]["K_filtration_only"]
        assert "description" in wf
        assert any("normalisation_method='skip'" in step for step in wf["steps"])
        assert any("filtration_method=" in step for step in wf["steps"])

    def test_gene_workflow_present(self):
        wf = _WORKFLOW_GUIDE["workflows"]["L_gene_workflow"]
        assert "description" in wf
        assert any("md_format_gene" in step for step in wf["steps"])
        assert any("entity_type='gene'" in step for step in wf["steps"])

    def test_common_mistakes_have_new_v3_entries(self):
        haystack = " ".join(_WORKFLOW_GUIDE["common_mistakes"])
        # Batch correction sub-technique guidance
        assert "BATCH CORRECTION TECHNIQUE" in haystack
        assert "combat seq" in haystack
        # Filtration value canonical-form guidance
        assert "FILTRATION VALUES" in haystack
        assert "by missing values" in haystack
        # Filter-only guidance
        assert "FILTER-ONLY PATTERN" in haystack
        # Gene-stat scope guidance (renamed to DE METHOD SCOPE in 2026-05; gene
        # now exposes edgeR / DESeq2 via de_method while non-gene stays limma-only).
        assert "DE METHOD SCOPE" in haystack
        assert "edgeR" in haystack and "DESeq2" in haystack
        # md_format_gene guidance
        assert "GENE UPLOAD SOURCE" in haystack
        assert "md_format_gene" in haystack

    def test_find_initial_dataset_description_mentions_disambiguation(self):
        desc = _WORKFLOW_GUIDE["tool_index"]["dataset_tools"]["find_initial_dataset"]
        assert "upload-created" in desc
        assert "no upstream input" in desc

    def test_overview_states_data_vs_workspace_boundary(self):
        overview = _WORKFLOW_GUIDE["overview"]
        assert "DATA vs WORKSPACE BOUNDARY" in overview
        assert "NO workspace association" in overview
        assert "REFERENCE existing datasets" in overview

    def test_visualise_workflow_states_workspace_is_not_a_data_container(self):
        desc = _WORKFLOW_GUIDE["workflows"]["M_visualise"]["description"]
        assert "does NOT own" in desc
        assert "REFERENCE existing" in desc
        assert "never create a workspace as a prerequisite" in desc

    def test_top_level_mcp_instructions_carry_boundary(self):
        from mcp_tools import mcp

        instructions = mcp.instructions or ""
        assert "DATA vs WORKSPACE BOUNDARY" in instructions
        assert (
            'NEVER ask the user "which workspace should I upload into"' in instructions
        )
        assert "NEVER create a workspace as a prerequisite" in instructions
