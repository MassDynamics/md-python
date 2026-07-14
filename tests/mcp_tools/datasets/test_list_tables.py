"""Tests for list_dataset_tables."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import list_dataset_tables
from md_python.resources.v2.datasets import DatasetNotFoundError


class TestListDatasetTables:
    def test_returns_verified_tables_for_dataset(self):
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.return_value = {
            "dataset_id": "ds-1",
            "type": "INTENSITY",
            "catalogued": True,
            "verified": True,
            "entity": "protein",
            "entity_resolved_from": "job_run_params",
            "candidates": ["Protein_Intensity", "Protein_Metadata"],
            "tables": ["Protein_Intensity", "Protein_Metadata"],
            "unavailable": [],
            "note": "...",
        }
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(list_dataset_tables("ds-1"))

        # verification is ON by default — "is the data there" must be answered
        mock_client.datasets.list_table_names.assert_called_once_with(
            "ds-1", verify=True, upload_id=None
        )
        assert result["type"] == "INTENSITY"
        assert result["catalogued"] is True
        assert result["verified"] is True
        assert result["entity"] == "protein"
        assert "Protein_Intensity" in result["tables"]

    def test_threads_verify_and_upload_id_through(self):
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.return_value = {
            "dataset_id": "ds-1",
            "catalogued": True,
            "verified": False,
            "candidates": ["Gene_Intensity", "Gene_Metadata"],
        }
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(
                list_dataset_tables("ds-1", verify=False, upload_id="up-1")
            )

        mock_client.datasets.list_table_names.assert_called_once_with(
            "ds-1", verify=False, upload_id="up-1"
        )
        assert result["verified"] is False
        assert "tables" not in result  # unverified names are candidates only

    def test_dataset_not_found_is_its_own_reason(self):
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.side_effect = DatasetNotFoundError(
            "Dataset 'ds-1' does not exist ... DELETED in the web UI ..."
        )
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(list_dataset_tables("ds-1"))

        assert result["reason"] == "dataset_not_found"
        assert result["dataset_id"] == "ds-1"
        assert "DELETED in the web UI" in result["error"]

    def test_returns_error_on_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.side_effect = Exception("boom")
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(list_dataset_tables("ds-1"))

        assert result["error"] == "boom"
        assert result["dataset_id"] == "ds-1"
        assert "reason" not in result  # cause unknown — do not claim one

    def test_surfaces_uncatalogued_flag_and_do_not_guess_note(self):
        # WGCNA stands in for a type that really cannot be enumerated —
        # ENRICHMENT/ORA/ANOVA are catalogued now.
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.return_value = {
            "dataset_id": "ds-1",
            "type": "WGCNA",
            "catalogued": False,
            "verified": False,
            "tables": [],
            "confirmed_tables": ["runtime_metadata"],
            "note": "DO NOT brute-force guess table names",
        }
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(list_dataset_tables("ds-1"))

        assert result["catalogued"] is False
        assert result["verified"] is False
        assert result["confirmed_tables"] == ["runtime_metadata"]
        assert "DO NOT brute-force guess" in result["note"]

    def test_enrichment_is_catalogued_and_verified(self):
        # The former dead end: catalogued=false meant "nothing to download".
        mock_client = MagicMock()
        mock_client.datasets.list_table_names.return_value = {
            "dataset_id": "ds-1",
            "type": "ENRICHMENT",
            "catalogued": True,
            "verified": True,
            "candidates": [
                "output_comparisons",
                "database_metadata",
                "runtime_metadata",
            ],
            "tables": ["output_comparisons", "database_metadata"],
            "unavailable": ["runtime_metadata"],
        }
        with patch(
            "mcp_tools.datasets.list_tables.get_client", return_value=mock_client
        ):
            result = json.loads(list_dataset_tables("ds-1"))

        assert result["catalogued"] is True
        assert result["tables"] == ["output_comparisons", "database_metadata"]


class TestDocstringNamesRealTools:
    """Failure mode 1: never point the model at a tool it cannot call."""

    def test_docstring_does_not_name_the_internal_sdk_method(self):
        assert "list_table_names" not in (list_dataset_tables.__doc__ or "")


class TestDocstringAdvertisesTheCatalogue:
    """The names must be discoverable BEFORE the model hits an error."""

    def test_docstring_lists_the_enrichment_ora_anova_tables(self):
        doc = list_dataset_tables.__doc__ or ""
        assert "ENRICHMENT" in doc
        assert "output_comparisons" in doc
        assert "database_metadata" in doc
        assert "ora_results" in doc
        assert "anova_results" in doc

    def test_docstring_flags_the_pairwise_name_collision(self):
        doc = list_dataset_tables.__doc__ or ""
        assert "SAME name PAIRWISE uses" in doc
        assert "output_gsea" in doc
