"""Tests for download_dataset_table."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import download_dataset_table
from md_python.resources.v2.datasets import (
    REASON_DATASET_NOT_FOUND,
    REASON_TABLE_NAME_INVALID,
    REASON_TABLE_NOT_IN_MODALITY,
    DatasetNotFoundError,
    TableNotFoundError,
)


def _intensity_catalogue(entity="protein"):
    tables = {
        "protein": ["Protein_Intensity", "Protein_Metadata"],
        "metabolite": ["Metabolite_Intensity", "Metabolite_Metadata"],
    }[entity]
    return {
        "dataset_id": "ds-1",
        "type": "INTENSITY",
        "catalogued": True,
        "verified": False,
        "entity": entity,
        "entity_resolved_from": "job_run_params",
        "candidates": tables,
    }


def _uncatalogued_catalogue(dataset_type="WGCNA"):
    # ENRICHMENT/ORA/ANOVA are catalogued now — WGCNA stands in for a type whose
    # tables genuinely cannot be enumerated.
    return {
        "dataset_id": "ds-1",
        "type": dataset_type,
        "catalogued": False,
        "verified": False,
        "tables": [],
        "note": "cannot be enumerated",
    }


def _enrichment_catalogue():
    return {
        "dataset_id": "ds-1",
        "type": "ENRICHMENT",
        "catalogued": True,
        "verified": False,
        "entity": None,
        "entity_resolved_from": None,
        "candidates": [
            "output_comparisons",
            "database_metadata",
            "runtime_metadata",
        ],
    }


def _client(catalogue=None, url="https://s3.example.com/presigned"):
    mock_client = MagicMock()
    mock_client.datasets.list_table_names.return_value = (
        catalogue if catalogue is not None else _intensity_catalogue()
    )
    mock_client.datasets.download_table_url.return_value = url
    return mock_client


class TestDownloadDatasetTable:
    def test_returns_url_when_no_output_path(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(
                download_dataset_table("ds-1", "Protein_Intensity", format="csv")
            )

        mock_client.datasets.download_table_url.assert_called_once_with(
            "ds-1", "Protein_Intensity", format="csv"
        )
        assert result["download_url"] == "https://s3.example.com/presigned"
        assert result["dataset_id"] == "ds-1"
        assert result["table_name"] == "Protein_Intensity"
        assert result["format"] == "csv"

    def test_writes_file_when_output_path_given(self, tmp_path):
        mock_client = _client()

        fake_response = MagicMock()
        fake_response.__enter__.return_value = fake_response
        fake_response.__exit__.return_value = False
        fake_response.raise_for_status.return_value = None
        fake_response.iter_content.return_value = [b"abc", b"defg"]

        out = tmp_path / "protein.csv"

        with (
            patch("mcp_tools.datasets.download.get_client", return_value=mock_client),
            patch(
                "mcp_tools.datasets.download.requests.get", return_value=fake_response
            ) as mock_get,
        ):
            result = json.loads(
                download_dataset_table(
                    "ds-1",
                    "Protein_Intensity",
                    format="csv",
                    output_path=str(out),
                )
            )

        mock_get.assert_called_once()
        assert result["path"] == str(out)
        assert result["bytes"] == 7
        assert out.read_bytes() == b"abcdefg"

    def test_rejects_invalid_format(self):
        result = json.loads(
            download_dataset_table("ds-1", "Protein_Intensity", format="xml")
        )
        assert "error" in result
        assert "Invalid format" in result["error"]

    def test_propagates_client_error(self):
        mock_client = _client()
        mock_client.datasets.download_table_url.side_effect = Exception(
            "Failed to get download URL: 404"
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Intensity"))
        assert "error" in result
        assert "404" in result["error"]

    def test_table_not_found_surfaces_actionable_message(self):
        mock_client = _client(catalogue=_uncatalogued_catalogue())
        mock_client.datasets.download_table_url.side_effect = TableNotFoundError(
            "Table 'output_enrichment' not found in dataset 'ds-1'. "
            "DO NOT brute-force guess table names."
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "output_enrichment"))
        assert "error" in result
        assert "DO NOT brute-force guess" in result["error"]
        # not buried behind the generic prefix
        assert "Failed to get download URL" not in result["error"]

    def test_propagates_stream_error(self, tmp_path):
        mock_client = _client()
        with (
            patch("mcp_tools.datasets.download.get_client", return_value=mock_client),
            patch(
                "mcp_tools.datasets.download.requests.get",
                side_effect=RuntimeError("network blew up"),
            ),
        ):
            result = json.loads(
                download_dataset_table(
                    "ds-1",
                    "Protein_Intensity",
                    output_path=str(tmp_path / "out.csv"),
                )
            )
        assert "error" in result
        assert "network blew up" in result["error"]


class TestPreflightGuard:
    """A wrong name on a catalogued type is a guaranteed 404 — never call out."""

    def test_unknown_table_on_catalogued_type_skips_http_call(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "output_intensity"))

        mock_client.datasets.download_table_url.assert_not_called()
        assert result["valid_tables"] == ["Protein_Intensity", "Protein_Metadata"]
        assert result["case_sensitive"] is True
        assert "Protein_Intensity" in result["error"]
        assert "case-sensitive" in result["error"]

    def test_case_mismatch_gets_did_you_mean_hint(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "protein_intensity"))

        mock_client.datasets.download_table_url.assert_not_called()
        assert result["did_you_mean"] == "Protein_Intensity"
        assert "Did you mean 'Protein_Intensity'?" in result["error"]
        assert "CASE-SENSITIVE" in result["error"]

    def test_valid_table_on_catalogued_type_passes_through(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Metadata"))

        mock_client.datasets.download_table_url.assert_called_once()
        assert "download_url" in result

    def test_uncatalogued_type_is_let_through(self):
        # An uncatalogued type's tables cannot be enumerated, so we cannot prove
        # the name is wrong — the request must still go out.
        mock_client = _client(catalogue=_uncatalogued_catalogue())
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "runtime_metadata"))

        mock_client.datasets.download_table_url.assert_called_once_with(
            "ds-1", "runtime_metadata", format="csv"
        )
        assert "download_url" in result

    def test_preflight_lookup_failure_lets_request_through(self):
        mock_client = _client()
        mock_client.datasets.list_table_names.side_effect = Exception("boom")
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "anything"))

        mock_client.datasets.download_table_url.assert_called_once()
        assert "download_url" in result

    def test_preflight_uses_the_cheap_unverified_lookup(self):
        # The download IS the probe — pre-flight must not probe every table.
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            download_dataset_table("ds-1", "Protein_Intensity")

        mock_client.datasets.list_table_names.assert_called_once_with(
            "ds-1", verify=False
        )

    def test_invalid_format_short_circuits_before_preflight(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(
                download_dataset_table("ds-1", "Protein_Intensity", format="xml")
            )

        mock_client.datasets.list_table_names.assert_not_called()
        assert "Invalid format" in result["error"]


class TestFailureReasons:
    """One 404, three causes. The model must be able to tell them apart."""

    def test_dead_dataset_is_reported_as_dataset_not_found(self):
        # Deleted in the web UI: the MCP is never notified.
        mock_client = _client()
        mock_client.datasets.list_table_names.side_effect = DatasetNotFoundError(
            "Dataset 'ds-1' does not exist ... DELETED in the web UI ... "
            "find_initial_dataset"
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Intensity"))

        mock_client.datasets.download_table_url.assert_not_called()
        assert result["reason"] == REASON_DATASET_NOT_FOUND
        assert "DELETED in the web UI" in result["error"]
        # a dead dataset must NOT be answered with table-name advice
        assert "valid_tables" not in result
        assert "did_you_mean" not in result

    def test_dataset_deleted_between_preflight_and_download(self):
        # Mid-session disappearance: existence is never cached.
        mock_client = _client()
        mock_client.datasets.download_table_url.side_effect = DatasetNotFoundError(
            "Dataset 'ds-1' does not exist ... DELETED in the web UI ..."
        )
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Intensity"))

        assert result["reason"] == REASON_DATASET_NOT_FOUND
        assert result["dataset_id"] == "ds-1"

    def test_bad_name_is_reported_as_table_name_invalid(self):
        mock_client = _client()
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "output_intensity"))

        assert result["reason"] == REASON_TABLE_NAME_INVALID
        assert result["valid_tables"] == ["Protein_Intensity", "Protein_Metadata"]

    def test_protein_table_on_metabolomics_is_a_modality_error(self):
        # The exact telemetry failure: protein asked of a dataset with no
        # protein layer. It is NOT a naming typo and must not read like one.
        mock_client = _client(catalogue=_intensity_catalogue("metabolite"))
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Intensity"))

        mock_client.datasets.download_table_url.assert_not_called()
        assert result["reason"] == REASON_TABLE_NOT_IN_MODALITY
        assert result["entity"] == "metabolite"
        assert result["valid_tables"] == [
            "Metabolite_Intensity",
            "Metabolite_Metadata",
        ]
        assert "metabolite dataset" in result["error"]
        assert "does not have" in result["error"]
        assert "did_you_mean" not in result

    def test_lowercase_protein_on_metabolomics_is_still_a_modality_error(self):
        mock_client = _client(catalogue=_intensity_catalogue("metabolite"))
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "protein_intensity"))

        assert result["reason"] == REASON_TABLE_NOT_IN_MODALITY
        assert "Metabolite_Intensity" in result["error"]

    def test_table_not_found_from_the_sdk_carries_its_reason(self):
        mock_client = _client(catalogue=_uncatalogued_catalogue())
        error = TableNotFoundError("no such table", REASON_TABLE_NOT_IN_MODALITY)
        mock_client.datasets.download_table_url.side_effect = error
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "Protein_Intensity"))

        assert result["reason"] == REASON_TABLE_NOT_IN_MODALITY
        assert result["table_name"] == "Protein_Intensity"


class TestEnrichmentDownload:
    """GSEA results are downloadable again — this used to be the dead end."""

    def test_output_comparisons_on_enrichment_is_accepted(self):
        # The name the model refused to try because it "means pairwise".
        mock_client = _client(catalogue=_enrichment_catalogue())
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "output_comparisons"))

        mock_client.datasets.download_table_url.assert_called_once_with(
            "ds-1", "output_comparisons", format="csv"
        )
        assert result["download_url"] == "https://s3.example.com/presigned"

    def test_database_metadata_on_enrichment_is_accepted(self):
        mock_client = _client(catalogue=_enrichment_catalogue())
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "database_metadata"))

        assert "download_url" in result

    def test_guessed_enrichment_name_is_rejected_with_the_valid_list(self):
        # output_gsea / output_enrichment / output_pathways were all guessed in
        # telemetry. Each must now come back with the real names, before the HTTP
        # call, instead of another bare 404.
        mock_client = _client(catalogue=_enrichment_catalogue())
        with patch("mcp_tools.datasets.download.get_client", return_value=mock_client):
            result = json.loads(download_dataset_table("ds-1", "output_gsea"))

        mock_client.datasets.download_table_url.assert_not_called()
        assert result["reason"] == REASON_TABLE_NAME_INVALID
        assert result["valid_tables"] == [
            "output_comparisons",
            "database_metadata",
            "runtime_metadata",
        ]
        assert "output_comparisons" in result["error"]
        assert "Do not try other names" in result["error"]


class TestDocstringAdvertisesTheEnrichmentNames:
    """Discoverability up front: the model should not need the error path."""

    def test_docstring_names_the_enrichment_tables(self):
        doc = download_dataset_table.__doc__ or ""
        assert "ENRICHMENT" in doc
        assert "output_comparisons" in doc
        assert "database_metadata" in doc
        assert "ora_results" in doc
        assert "anova_results" in doc

    def test_docstring_warns_off_the_guessed_names(self):
        doc = download_dataset_table.__doc__ or ""
        assert "output_gsea" in doc
        assert "do not" in doc.lower()
