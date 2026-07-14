"""The workflow guide must make the GSEA/ORA/ANOVA table names discoverable.

The guide is what the model reads FIRST (get_workflow_guide). While ENRICHMENT
was uncatalogued the guide said its tables "CANNOT be enumerated", and telemetry
shows the model then guessing twelve table names and abandoning the task. The
entries must now carry the real names — and the ANOVA query-filter trap.
"""

from mcp_tools.health import _WORKFLOW_GUIDE

_DATASET_TOOLS = _WORKFLOW_GUIDE["tool_index"]["dataset_tools"]


class TestListDatasetTablesEntry:
    def test_advertises_the_enrichment_ora_anova_names(self):
        entry = _DATASET_TOOLS["list_dataset_tables"]
        assert "ENRICHMENT" in entry
        assert "output_comparisons" in entry
        assert "database_metadata" in entry
        assert "ora_results" in entry
        assert "anova_results" in entry

    def test_no_longer_calls_enrichment_uncatalogued(self):
        entry = _DATASET_TOOLS["list_dataset_tables"]
        assert "other types (e.g. ENRICHMENT, ANOVA) return catalogued=false" not in (
            entry
        )
        # the do-not-guess advice survives, but only for genuinely unknown types
        assert "Any other type returns catalogued=false" in entry


class TestDownloadDatasetTableEntry:
    def test_names_the_gsea_results_table_and_the_collision(self):
        entry = _DATASET_TOOLS["download_dataset_table"]
        assert "output_comparisons" in entry
        assert "SAME name PAIRWISE uses" in entry
        assert "output_gsea" in entry  # explicitly warned off


class TestQueryDatasetsEntry:
    def test_documents_the_type_filter_enum_and_the_anova_trap(self):
        entry = _DATASET_TOOLS["query_datasets"]
        for value in (
            "DEMO",
            "DOSE_RESPONSE",
            "DOSE_RESPONSE_AGGREGATE",
            "ENRICHMENT",
            "IMPUTATION",
            "INTENSITY",
            "NORMALISATION_AND_IMPUTATION",
            "PAIRWISE",
        ):
            assert value in entry
        assert "400" in entry
        assert "ANOVA datasets DO exist" in entry
