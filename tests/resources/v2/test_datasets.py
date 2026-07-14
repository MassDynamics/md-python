from unittest.mock import Mock
from uuid import UUID

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import Dataset
from md_python.resources.v2.datasets import (
    REASON_TABLE_NAME_INVALID,
    REASON_TABLE_NOT_IN_MODALITY,
    DatasetNotFoundError,
    Datasets,
    TableNotFoundError,
    find_case_insensitive_match,
    invalid_table_message,
    uncatalogued_table_message,
)


def _dataset_response(dataset_type, job_run_params=None):
    """A 200 GET /datasets/:id response for a dataset of ``dataset_type``."""
    resp = Mock(status_code=200)
    resp.json.return_value = {
        "id": "11111111-1111-1111-1111-111111111111",
        "name": "DS",
        "type": dataset_type,
        "input_dataset_ids": [],
        "job_run_params": job_run_params or {},
    }
    return resp


def _table_url_response(location="https://s3.example.com/presigned"):
    return Mock(status_code=302, headers={"Location": location})


class TestV2Datasets:

    @pytest.fixture
    def mock_client(self):
        client = Mock(spec=MDClientV2)
        client.uploads = Mock()  # wired in MDClientV2.__init__, not on the class
        return client

    @pytest.fixture
    def datasets(self, mock_client):
        return Datasets(mock_client)

    @pytest.fixture
    def sample_dataset(self):
        return Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Test dataset",
            job_slug="demo_flow",
            job_run_params={"param": "value"},
        )

    def test_create_success(self, datasets, sample_dataset, mock_client):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"dataset_id": "abc123"}
        mock_client._make_request.return_value = mock_response

        result = datasets.create(sample_dataset)

        assert result == "abc123"
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets"

        payload = call_args[1]["json"]
        assert "dataset" not in payload
        assert payload["name"] == "Test dataset"
        assert payload["job_slug"] == "demo_flow"
        assert payload["input_dataset_ids"] == ["2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"]
        assert payload["job_run_params"] == {"param": "value"}

    def test_create_uses_flat_payload(self, datasets, sample_dataset, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"dataset_id": "flat-id"}
        mock_client._make_request.return_value = mock_response

        datasets.create(sample_dataset)

        payload = mock_client._make_request.call_args[1]["json"]
        assert "dataset" not in payload
        assert "name" in payload
        assert "job_slug" in payload

    def test_create_does_not_include_sample_names(self, datasets, mock_client):
        dataset = Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="No samples",
            job_slug="demo_flow",
            job_run_params={},
            sample_names=["s1", "s2"],
        )
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"dataset_id": "no-samples"}
        mock_client._make_request.return_value = mock_response

        datasets.create(dataset)

        payload = mock_client._make_request.call_args[1]["json"]
        assert "sample_names" not in payload

    def test_create_failure(self, datasets, sample_dataset, mock_client):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to create dataset: 400"):
            datasets.create(sample_dataset)

    def test_list_by_upload_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "a1b2c3d4e5f67890a1b2c3d4e5f67890",
                    "name": "DS1",
                    "job_slug": "flow_1",
                    "job_run_params": {},
                }
            ],
            "pagination": {"page": 1},
        }
        mock_client._make_request.return_value = mock_response

        result = datasets.list_by_upload("upload-1")

        assert len(result) == 1
        assert isinstance(result[0], Dataset)
        assert result[0].name == "DS1"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets/query"
        assert call_args[1]["json"] == {"upload_id": "upload-1"}

    def test_list_by_upload_empty(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [], "pagination": {}}
        mock_client._make_request.return_value = mock_response

        result = datasets.list_by_upload("upload-1")

        assert result == []

    def test_list_by_upload_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get datasets: 500"):
            datasets.list_by_upload("upload-1")

    def test_delete_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 204
        mock_client._make_request.return_value = mock_response

        result = datasets.delete("ds-1")

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "DELETE"
        assert call_args[1]["endpoint"] == "/datasets/ds-1"

    def test_delete_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to delete dataset: 404"):
            datasets.delete("ds-1")

    def test_retry_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        result = datasets.retry("ds-1")

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets/ds-1/retry"

    def test_retry_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to retry dataset: 500"):
            datasets.retry("ds-1")

    def test_cancel_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        result = datasets.cancel("ds-1")

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets/ds-1/cancel"

    def test_cancel_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Cannot cancel"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to cancel dataset: 400"):
            datasets.cancel("ds-1")

    def test_get_by_id_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS1",
            "job_slug": "flow_1",
            "job_run_params": {},
            "input_dataset_ids": [],
        }
        mock_client._make_request.return_value = mock_response

        result = datasets.get_by_id("11111111-1111-1111-1111-111111111111")

        assert isinstance(result, Dataset)
        assert result.name == "DS1"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "GET"
        assert (
            call_args[1]["endpoint"] == "/datasets/11111111-1111-1111-1111-111111111111"
        )

    def test_get_by_id_not_found(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_client._make_request.return_value = mock_response

        result = datasets.get_by_id("nonexistent")

        assert result is None

    def test_get_by_id_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get dataset: 500"):
            datasets.get_by_id("ds-1")

    def test_download_table_url_success(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 302
        mock_response.headers = {"Location": "https://s3.amazonaws.com/presigned-url"}
        mock_client._make_request.return_value = mock_response

        result = datasets.download_table_url("ds-1", "intensity", format="csv")

        assert result == "https://s3.amazonaws.com/presigned-url"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == "/datasets/ds-1/tables/intensity.csv"
        assert call_args[1]["allow_redirects"] is False

    def test_download_table_url_parquet(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 302
        mock_response.headers = {
            "Location": "https://s3.amazonaws.com/presigned-parquet"
        }
        mock_client._make_request.return_value = mock_response

        result = datasets.download_table_url("ds-1", "intensity", format="parquet")

        assert result == "https://s3.amazonaws.com/presigned-parquet"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["endpoint"] == "/datasets/ds-1/tables/intensity.parquet"

    def test_download_table_url_invalid_format(self, datasets):
        with pytest.raises(ValueError, match="format must be 'csv' or 'parquet'"):
            datasets.download_table_url("ds-1", "intensity", format="json")

    def test_download_table_url_404_on_dead_dataset_says_dataset_not_found(
        self, datasets, mock_client
    ):
        # 404 on the table fetch AND on the dataset lookup: the DATASET is
        # gone (deleted in the web UI), not the table. Saying anything about
        # table names here is what sent the model guessing.
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(DatasetNotFoundError) as exc:
            datasets.download_table_url("ds-1", "intensity")

        msg = str(exc.value)
        assert "does not exist" in msg
        assert "DELETED in the web UI" in msg
        assert "find_initial_dataset" in msg
        assert "do NOT try other table names" in msg
        assert "Protein_Intensity" not in msg
        # ValueError subclass so existing broad catches still work
        assert isinstance(exc.value, ValueError)
        assert not isinstance(exc.value, TableNotFoundError)

    def test_download_table_url_404_lists_available_tables(self, datasets, mock_client):
        # First call (table fetch) 404s; second call (get_by_id) returns an
        # INTENSITY dataset so the error lists the valid capitalised names.
        table_404 = Mock(status_code=404, text="Not found")
        ds_resp = Mock(status_code=200)
        ds_resp.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS",
            "type": "INTENSITY",
            "input_dataset_ids": [],
        }
        mock_client._make_request.side_effect = [table_404, ds_resp]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "protein_intensity")

        msg = str(exc.value)
        assert "Protein_Intensity" in msg
        assert "Metabolite_Metadata" in msg
        # case-only mismatch is the most common mistake — call it out
        assert "Did you mean 'Protein_Intensity'?" in msg
        assert "CASE-SENSITIVE" in msg

    def test_table_not_found_messages_name_the_mcp_tool(self, datasets, mock_client):
        # list_table_names is an internal SDK method; the tool the model can
        # actually call over MCP is list_dataset_tables.
        table_404 = Mock(status_code=404, text="Not found")
        ds_resp = Mock(status_code=200)
        ds_resp.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS",
            "type": "PAIRWISE",
            "input_dataset_ids": [],
        }
        mock_client._make_request.side_effect = [table_404, ds_resp]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "comparisons")

        msg = str(exc.value)
        assert "list_dataset_tables" in msg
        assert "list_table_names" not in msg

    def test_table_not_found_message_when_lookup_fails_names_mcp_tool(
        self, datasets, mock_client
    ):
        # The table 404s and the follow-up catalogue lookup itself blows up
        # (5xx, not a 404): we cannot name the cause, but the message must
        # still point at the discovery tool.
        table_404 = Mock(status_code=404, text="Not found")
        ds_500 = Mock(status_code=500, text="Server error")
        mock_client._make_request.side_effect = [table_404, ds_500]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "intensity")

        msg = str(exc.value)
        assert "list_dataset_tables" in msg
        assert "list_table_names" not in msg

    def test_table_not_found_on_uncatalogued_type_forbids_guessing(
        self, datasets, mock_client
    ):
        table_404 = Mock(status_code=404, text="Not found")
        ds_resp = Mock(status_code=200)
        ds_resp.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS",
            "type": "ENRICHMENT",
            "input_dataset_ids": [],
        }
        mock_client._make_request.side_effect = [table_404, ds_resp]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "output_enrichment")

        msg = str(exc.value)
        assert "CANNOT be enumerated" in msg
        assert "DO NOT brute-force guess" in msg
        assert "runtime_metadata" in msg
        assert "ask them for the exact table name" in msg

    def test_download_table_url_docstring_names_the_mcp_tool(self, datasets):
        doc = Datasets.download_table_url.__doc__ or ""
        assert "list_dataset_tables" in doc
        assert "list_table_names" not in doc

    def test_list_table_names_intensity_unresolved_entity_returns_union(
        self, datasets, mock_client
    ):
        # No entity_type, no upload: the modality is unknown, so every entity's
        # tables are CANDIDATES — and the result must say so, not pretend they
        # exist.
        mock_client._make_request.return_value = _dataset_response("INTENSITY")

        result = datasets.list_table_names("ds-1", verify=False)

        assert result["type"] == "INTENSITY"
        assert result["entity"] is None
        assert result["entity_resolved_from"] is None
        assert "Protein_Intensity" in result["candidates"]
        assert "Metabolite_Intensity" in result["candidates"]
        assert "tables" not in result  # unverified => nothing is "present"
        assert result["verified"] is False
        assert "could NOT be resolved" in result["note"]
        assert result["tables_by_entity"]["gene"] == [
            "Gene_Intensity",
            "Gene_Metadata",
        ]

    def test_list_table_names_pairwise(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response("PAIRWISE")

        result = datasets.list_table_names("ds-1", verify=False)

        assert result["candidates"] == ["output_comparisons", "runtime_metadata"]
        assert "tables_by_entity" not in result

    def test_list_table_names_intensity_is_catalogued(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response("INTENSITY")

        assert datasets.list_table_names("ds-1", verify=False)["catalogued"] is True

    def test_list_table_names_uncatalogued_type_says_so(self, datasets, mock_client):
        # ENRICHMENT has no verified catalogue. Returning an empty list reads
        # as "no tables" and provoked 12 consecutive 404 guesses in telemetry.
        ds_resp = Mock(status_code=200)
        ds_resp.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS",
            "type": "ENRICHMENT",
            "input_dataset_ids": [],
        }
        mock_client._make_request.return_value = ds_resp

        result = datasets.list_table_names("ds-1")

        assert result["catalogued"] is False
        assert result["tables"] == []
        assert "ENRICHMENT" in result["note"]
        assert "CANNOT be enumerated" in result["note"]
        assert "DO NOT brute-force guess" in result["note"]
        # the one ENRICHMENT table confirmed to download, flagged non-exhaustive
        assert result["confirmed_tables"] == ["runtime_metadata"]
        assert "NON-EXHAUSTIVE" in result["confirmed_tables_note"]

    def test_list_table_names_uncatalogued_type_without_confirmed(
        self, datasets, mock_client
    ):
        ds_resp = Mock(status_code=200)
        ds_resp.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "DS",
            "type": "ANOVA",
            "input_dataset_ids": [],
        }
        mock_client._make_request.return_value = ds_resp

        result = datasets.list_table_names("ds-1")

        assert result["catalogued"] is False
        assert "confirmed_tables" not in result
        assert "DO NOT brute-force guess" in result["note"]

    def test_list_table_names_dataset_not_found(self, datasets, mock_client):
        mock_client._make_request.return_value = Mock(status_code=404)

        with pytest.raises(DatasetNotFoundError) as exc:
            datasets.list_table_names("ds-1")

        msg = str(exc.value)
        assert "DELETED in the web UI" in msg
        assert "NOT a table-name problem" in msg


class TestEntityNarrowing:
    """An INTENSITY dataset only holds ITS entity's tables — narrow to it."""

    @pytest.fixture
    def mock_client(self):
        client = Mock(spec=MDClientV2)
        client.uploads = Mock()  # wired in MDClientV2.__init__, not on the class
        return client

    @pytest.fixture
    def datasets(self, mock_client):
        return Datasets(mock_client)

    def test_entity_from_job_run_params(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response(
            "INTENSITY", {"entity_type": "gene"}
        )

        result = datasets.list_table_names("ds-1", verify=False)

        assert result["entity"] == "gene"
        assert result["entity_resolved_from"] == "job_run_params"
        assert result["candidates"] == ["Gene_Intensity", "Gene_Metadata"]
        assert "Protein_Intensity" not in result["candidates"]
        # no per-entity union when we know the modality
        assert "tables_by_entity" not in result
        mock_client.uploads.get_by_id.assert_not_called()

    def test_entity_from_upload_source(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response("INTENSITY")
        mock_client.uploads.get_by_id.return_value = Mock(source="md_format_metabolite")

        result = datasets.list_table_names("ds-1", verify=False, upload_id="up-1")

        assert result["entity"] == "metabolite"
        assert result["entity_resolved_from"] == "upload_source"
        assert result["candidates"] == [
            "Metabolite_Intensity",
            "Metabolite_Metadata",
        ]
        mock_client.uploads.get_by_id.assert_called_once_with("up-1")

    def test_upload_id_from_job_run_params_is_used(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response(
            "INTENSITY", {"upload_id": "up-9"}
        )
        mock_client.uploads.get_by_id.return_value = Mock(source="md_format_gene")

        result = datasets.list_table_names("ds-1", verify=False)

        assert result["entity"] == "gene"
        mock_client.uploads.get_by_id.assert_called_once_with("up-9")

    def test_proteomics_source_is_ambiguous_so_nothing_is_narrowed(
        self, datasets, mock_client
    ):
        # md_format is protein AND maybe peptide — collapsing it to protein
        # would be a guess, which is the bug we are fixing.
        mock_client._make_request.return_value = _dataset_response("INTENSITY")
        mock_client.uploads.get_by_id.return_value = Mock(source="md_format")

        result = datasets.list_table_names("ds-1", verify=False, upload_id="up-1")

        assert result["entity"] is None
        assert result["entity_resolved_from"] is None
        assert "Protein_Intensity" in result["candidates"]
        assert "Peptide_Intensity" in result["candidates"]

    def test_unresolvable_entity_falls_back_to_union_and_says_so(
        self, datasets, mock_client
    ):
        mock_client._make_request.return_value = _dataset_response("INTENSITY")
        mock_client.uploads.get_by_id.side_effect = Exception("upload lookup failed")

        result = datasets.list_table_names("ds-1", verify=False, upload_id="up-1")

        assert result["entity_resolved_from"] is None
        assert len(result["candidates"]) == 11  # every entity's tables
        assert "could NOT be resolved" in result["note"]

    def test_unknown_entity_type_value_is_not_trusted(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response(
            "INTENSITY", {"entity_type": "lipid"}
        )

        result = datasets.list_table_names("ds-1", verify=False)

        assert result["entity"] is None
        assert "Protein_Intensity" in result["candidates"]


class TestTableVerification:
    """verify=True answers 'is the data there', not 'what could it be called'."""

    @pytest.fixture
    def mock_client(self):
        client = Mock(spec=MDClientV2)
        client.uploads = Mock()  # wired in MDClientV2.__init__, not on the class
        return client

    @pytest.fixture
    def datasets(self, mock_client):
        return Datasets(mock_client)

    def test_verify_splits_present_from_absent(self, datasets, mock_client):
        ds = _dataset_response("INTENSITY", {"entity_type": "metabolite"})
        mock_client._make_request.side_effect = [
            ds,  # get_by_id
            _table_url_response(),  # Metabolite_Intensity -> 302, present
            Mock(status_code=404, text="Not found"),  # Metabolite_Metadata -> absent
            ds,  # the 404 message re-reads the catalogue (verify=False)
        ]

        result = datasets.list_table_names("ds-1")

        assert result["verified"] is True
        assert result["tables"] == ["Metabolite_Intensity"]
        assert result["unavailable"] == ["Metabolite_Metadata"]
        assert result["candidates"] == [
            "Metabolite_Intensity",
            "Metabolite_Metadata",
        ]
        assert "indeterminate" not in result
        assert "CONFIRMED to exist" in result["verification_note"]

    def test_verify_false_probes_nothing(self, datasets, mock_client):
        mock_client._make_request.return_value = _dataset_response(
            "INTENSITY", {"entity_type": "gene"}
        )

        result = datasets.list_table_names("ds-1", verify=False)

        # exactly one request: the dataset lookup. No table probes.
        assert mock_client._make_request.call_count == 1
        assert result["verified"] is False
        assert "tables" not in result
        assert "unavailable" not in result

    def test_non_404_probe_error_is_indeterminate_not_absent(
        self, datasets, mock_client
    ):
        # A 5xx/network failure is NOT evidence the table is missing. Calling
        # it absent would be a lie.
        mock_client._make_request.side_effect = [
            _dataset_response("INTENSITY", {"entity_type": "gene"}),
            _table_url_response(),  # Gene_Intensity present
            Mock(status_code=503, text="upstream unavailable"),  # Gene_Metadata
        ]

        result = datasets.list_table_names("ds-1")

        assert result["tables"] == ["Gene_Intensity"]
        assert result["unavailable"] == []
        assert result["indeterminate"] == [
            {
                "table": "Gene_Metadata",
                "error": "Failed to get download URL: 503 - upstream unavailable",
            }
        ]
        assert "UNKNOWN" in result["indeterminate_note"]

    def test_dataset_deleted_mid_probe_raises_dataset_not_found(
        self, datasets, mock_client
    ):
        # The dataset can be deleted from under us mid-session.
        mock_client._make_request.side_effect = [
            _dataset_response("INTENSITY", {"entity_type": "gene"}),
            Mock(status_code=404, text="Not found"),  # probe 404s
            Mock(status_code=404),  # ...because the dataset is now gone
        ]

        with pytest.raises(DatasetNotFoundError):
            datasets.list_table_names("ds-1")

    def test_uncatalogued_type_is_never_probed(self, datasets, mock_client):
        # No candidate names => nothing to verify. Do not invent names.
        mock_client._make_request.return_value = _dataset_response("ENRICHMENT")

        result = datasets.list_table_names("ds-1", verify=True)

        assert mock_client._make_request.call_count == 1
        assert result["catalogued"] is False
        assert result["verified"] is False
        assert result["tables"] == []
        assert "CANNOT be enumerated" in result["note"]
        assert "NOT because the dataset has no tables" in result["tables_note"]


class TestModalityRejection:
    """Asking for another modality's table is cause (c), not a naming typo."""

    @pytest.fixture
    def mock_client(self):
        client = Mock(spec=MDClientV2)
        client.uploads = Mock()  # wired in MDClientV2.__init__, not on the class
        return client

    @pytest.fixture
    def datasets(self, mock_client):
        return Datasets(mock_client)

    def test_protein_table_on_metabolomics_dataset(self, datasets, mock_client):
        ds = _dataset_response("INTENSITY", {"entity_type": "metabolite"})
        mock_client._make_request.side_effect = [
            Mock(status_code=404, text="Not found"),
            ds,
        ]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "Protein_Intensity")

        assert exc.value.reason == REASON_TABLE_NOT_IN_MODALITY
        msg = str(exc.value)
        assert "this is a metabolite dataset" in msg.lower()
        assert "Metabolite_Intensity" in msg
        assert "does not have" in msg

    def test_case_mismatch_still_wins_on_the_right_modality(
        self, datasets, mock_client
    ):
        ds = _dataset_response("INTENSITY", {"entity_type": "protein"})
        mock_client._make_request.side_effect = [
            Mock(status_code=404, text="Not found"),
            ds,
        ]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "protein_intensity")

        assert exc.value.reason == REASON_TABLE_NAME_INVALID
        assert "Did you mean 'Protein_Intensity'?" in str(exc.value)

    def test_valid_name_that_still_404s_is_not_a_naming_problem(
        self, datasets, mock_client
    ):
        ds = _dataset_response("PAIRWISE")
        mock_client._make_request.side_effect = [
            Mock(status_code=404, text="Not found"),
            ds,
        ]

        with pytest.raises(TableNotFoundError) as exc:
            datasets.download_table_url("ds-1", "output_comparisons")

        msg = str(exc.value)
        assert "the run did not produce it" in msg
        assert "do not guess other names" in msg

    def test_query_with_all_filters(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"name": "DS1", "job_slug": "flow_1"}],
            "pagination": {"page": 1, "total_pages": 1},
        }
        mock_client._make_request.return_value = mock_response

        result = datasets.query(
            upload_id="upload-1",
            state=["COMPLETED"],
            type=["INTENSITY"],
            search="test",
            page=2,
        )

        assert result["data"][0]["name"] == "DS1"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets/query"

        payload = call_args[1]["json"]
        assert payload["upload_id"] == "upload-1"
        assert payload["state"] == ["COMPLETED"]
        assert payload["type"] == ["INTENSITY"]
        assert payload["search"] == "test"
        assert payload["page"] == 2

    def test_query_with_defaults(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [], "pagination": {}}
        mock_client._make_request.return_value = mock_response

        datasets.query()

        payload = mock_client._make_request.call_args[1]["json"]
        assert payload == {"page": 1}

    def test_query_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to query datasets: 500"):
            datasets.query()

    def test_wait_until_complete_success(self, datasets, mock_client, mocker):
        completed_ds = Dataset(
            input_dataset_ids=[],
            name="n",
            job_slug="j",
            job_run_params={},
            state="COMPLETED",
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        mocker.patch.object(datasets, "get_by_id", return_value=completed_ds)

        result = datasets.wait_until_complete(
            "upload-1", "11111111-1111-1111-1111-111111111111", poll_s=0, timeout_s=1
        )

        assert isinstance(result, Dataset)

    def test_wait_until_complete_failure(self, datasets, mock_client, mocker):
        failed_ds = Dataset(
            input_dataset_ids=[],
            name="n",
            job_slug="j",
            job_run_params={},
            state="FAILED",
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        mocker.patch.object(datasets, "get_by_id", return_value=failed_ds)

        with pytest.raises(Exception, match="failed"):
            datasets.wait_until_complete(
                "upload-1",
                "11111111-1111-1111-1111-111111111111",
                poll_s=0,
                timeout_s=1,
            )

    def test_wait_until_complete_not_visible_then_completed(
        self, datasets, mock_client, mocker
    ):
        """Regression: wait must use get_by_id so uploads with >50 sibling
        datasets still find the target. Under the old list_by_upload
        implementation a dataset beyond the first page would never be seen."""
        completed_ds = Dataset(
            input_dataset_ids=[],
            name="n",
            job_slug="j",
            job_run_params={},
            state="COMPLETED",
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        get_by_id = mocker.patch.object(
            datasets, "get_by_id", side_effect=[None, completed_ds]
        )

        result = datasets.wait_until_complete(
            "upload-1", "11111111-1111-1111-1111-111111111111", poll_s=0, timeout_s=5
        )

        assert result is completed_ds
        assert get_by_id.call_count == 2

    def test_find_initial_dataset_success(self, datasets, mock_client, mocker):
        ds = Dataset(
            input_dataset_ids=[],
            name="n",
            job_slug="j",
            job_run_params={},
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        ds.type = "INTENSITY"
        mocker.patch.object(datasets, "list_by_upload", return_value=[ds])

        result = datasets.find_initial_dataset("upload-1")

        assert result is ds

    def test_find_initial_dataset_no_datasets(self, datasets, mock_client, mocker):
        mocker.patch.object(datasets, "list_by_upload", return_value=[])

        with pytest.raises(ValueError, match="No datasets found"):
            datasets.find_initial_dataset("upload-1")

    def test_find_initial_dataset_no_intensity(self, datasets, mock_client, mocker):
        ds = Dataset(
            input_dataset_ids=[],
            name="n",
            job_slug="j",
            job_run_params={},
        )
        ds.type = "OTHER"
        mocker.patch.object(datasets, "list_by_upload", return_value=[ds])

        with pytest.raises(ValueError, match="No intensity dataset"):
            datasets.find_initial_dataset("upload-1")

    def test_find_initial_dataset_disambiguates_after_ni_run(
        self, datasets, mock_client, mocker
    ):
        """After running NI, an upload has 2+ INTENSITY datasets.

        The original is the unique one with empty input_dataset_ids.
        """
        original = Dataset(
            input_dataset_ids=[],
            name="upload INTENSITY",
            job_slug="initial",
            job_run_params={},
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        original.type = "INTENSITY"

        ni_output = Dataset(
            input_dataset_ids=[UUID("11111111-1111-1111-1111-111111111111")],
            name="post-NI INTENSITY",
            job_slug="normalisation_imputation",
            job_run_params={},
            id=UUID("22222222-2222-2222-2222-222222222222"),
        )
        ni_output.type = "INTENSITY"

        mocker.patch.object(
            datasets, "list_by_upload", return_value=[original, ni_output]
        )

        result = datasets.find_initial_dataset("upload-1")
        assert result is original

    def test_find_initial_dataset_raises_when_multiple_originals(
        self, datasets, mock_client, mocker
    ):
        """Two upload-created INTENSITY datasets is still ambiguous."""
        a = Dataset(
            input_dataset_ids=[],
            name="a",
            job_slug="j",
            job_run_params={},
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        a.type = "INTENSITY"
        b = Dataset(
            input_dataset_ids=[],
            name="b",
            job_slug="j",
            job_run_params={},
            id=UUID("22222222-2222-2222-2222-222222222222"),
        )
        b.type = "INTENSITY"
        mocker.patch.object(datasets, "list_by_upload", return_value=[a, b])

        with pytest.raises(ValueError, match="Multiple upload-created"):
            datasets.find_initial_dataset("upload-1")

    def test_find_initial_dataset_raises_when_no_originals(
        self, datasets, mock_client, mocker
    ):
        """Every INTENSITY has upstream inputs (original was deleted, etc.)."""
        a = Dataset(
            input_dataset_ids=[UUID("33333333-3333-3333-3333-333333333333")],
            name="a",
            job_slug="j",
            job_run_params={},
            id=UUID("11111111-1111-1111-1111-111111111111"),
        )
        a.type = "INTENSITY"
        b = Dataset(
            input_dataset_ids=[UUID("44444444-4444-4444-4444-444444444444")],
            name="b",
            job_slug="j",
            job_run_params={},
            id=UUID("22222222-2222-2222-2222-222222222222"),
        )
        b.type = "INTENSITY"
        mocker.patch.object(datasets, "list_by_upload", return_value=[a, b])

        with pytest.raises(ValueError, match="none of them is the upload-created"):
            datasets.find_initial_dataset("upload-1")


class TestTableNameHelpers:

    @pytest.mark.parametrize(
        "guess,expected",
        [
            ("protein_intensity", "Protein_Intensity"),
            ("PROTEIN_INTENSITY", "Protein_Intensity"),
            ("Protein_Intensity", None),  # exact match is not a "did you mean"
            ("output_enrichment", None),
        ],
    )
    def test_find_case_insensitive_match(self, guess, expected):
        tables = ["Protein_Intensity", "Protein_Metadata"]
        assert find_case_insensitive_match(guess, tables) == expected

    def test_invalid_table_message_lists_valid_names_and_forbids_guessing(self):
        msg = invalid_table_message(
            "ds-1", "output_comparison", "PAIRWISE", ["output_comparisons"]
        )
        assert "output_comparisons" in msg
        assert "case-sensitive" in msg
        assert "Do not try other names" in msg
        assert "list_dataset_tables" in msg

    def test_uncatalogued_table_message_forbids_guessing(self):
        msg = uncatalogued_table_message("ds-1", "output_gsea", "ENRICHMENT")
        assert "DO NOT brute-force guess" in msg
        assert "dozen calls" in msg
        assert "visualisation module" in msg
