from unittest.mock import Mock, patch

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import Dataset, ExperimentDesign, SampleMetadata, Upload
from md_python.resources.v2.uploads import Uploads
from src.md_python.models.upload import Source

DESIGN = ExperimentDesign(
    data=[
        ["filename", "sample_name", "condition"],
        ["a.txt", "s1", "ctrl"],
    ]
)

METADATA = SampleMetadata(
    data=[
        ["sample_name", "dose"],
        ["s1", "1"],
    ]
)


class TestV2Uploads:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def uploads(self, mock_client):
        return Uploads(mock_client)

    def test_create_with_s3_bucket(self, uploads, mock_client):
        upload = Upload(
            name="S3 Upload",
            source="maxquant",
            s3_bucket="my-bucket",
            s3_prefix="data/",
            filenames=["a.txt"],
            experiment_design=DESIGN,
            sample_metadata=METADATA,
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-123"}
        mock_client._make_request.return_value = mock_response

        result = uploads.create(upload)

        assert result == "upload-123"
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/uploads"

        payload = call_args[1]["json"]
        assert payload["name"] == "S3 Upload"
        assert payload["s3_bucket"] == "my-bucket"
        assert payload["s3_prefix"] == "data/"
        assert payload["experiment_design"] == DESIGN.data
        assert payload["sample_metadata"] == METADATA.data

    def test_create_with_tags(self, uploads, mock_client):
        upload = Upload(
            name="Tagged Upload",
            source="maxquant",
            s3_bucket="my-bucket",
            s3_prefix="data/",
            filenames=["a.txt"],
            sample_metadata=METADATA,
            tags=["pilot", "plasma"],
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-123"}
        mock_client._make_request.return_value = mock_response

        uploads.create(upload)

        payload = mock_client._make_request.call_args[1]["json"]
        assert payload["tags"] == ["pilot", "plasma"]

    def test_create_sends_description_and_labelling_method(self, uploads, mock_client):
        upload = Upload(
            name="Described Upload",
            description="TMT time course",
            source="diann_tabular",
            labelling_method="tmt",
            s3_bucket="my-bucket",
            filenames=["a.txt"],
            sample_metadata=METADATA,
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-123"}
        mock_client._make_request.return_value = mock_response

        uploads.create(upload)

        payload = mock_client._make_request.call_args[1]["json"]
        assert payload["description"] == "TMT time course"
        assert payload["labelling_method"] == "tmt"

    def test_create_without_tags_omits_key(self, uploads, mock_client):
        upload = Upload(
            name="Untagged Upload",
            source="maxquant",
            s3_bucket="my-bucket",
            filenames=["a.txt"],
            sample_metadata=METADATA,
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-123"}
        mock_client._make_request.return_value = mock_response

        uploads.create(upload)

        payload = mock_client._make_request.call_args[1]["json"]
        assert "tags" not in payload

    def test_upload_from_json_parses_tags(self):
        upload = Upload.from_json(
            {"name": "x", "source": "maxquant", "tags": ["pilot", "plasma"]}
        )
        assert upload.tags == ["pilot", "plasma"]
        assert "Tags: pilot, plasma" in str(upload)

    def test_upload_from_json_without_tags(self):
        upload = Upload.from_json({"name": "x", "source": "maxquant"})
        assert upload.tags is None

    def test_create_with_file_location(self, uploads, mock_client):
        upload = Upload(
            name="Local Upload",
            source="maxquant",
            file_location="/tmp/files",
            filenames=["data.raw"],
            experiment_design=DESIGN,
            sample_metadata=METADATA,
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-456"}
        mock_client._make_request.return_value = mock_response

        with patch.object(uploads._uploader, "file_sizes_for_api", return_value=[None]):
            result = uploads.create(upload)

        assert result == "upload-456"
        payload = mock_client._make_request.call_args[1]["json"]
        assert payload["file_sizes"] == [None]

    def test_create_with_file_upload_triggers_workflow(self, uploads, mock_client):
        upload = Upload(
            name="Upload With Files",
            source="maxquant",
            file_location="/tmp/files",
            filenames=["data.raw"],
            experiment_design=DESIGN,
            sample_metadata=METADATA,
        )

        create_response = Mock()
        create_response.status_code = 201
        create_response.json.return_value = {
            "id": "upload-789",
            "uploads": [{"filename": "data.raw", "url": "https://s3/presigned"}],
        }

        workflow_response = Mock()
        workflow_response.status_code = 200

        mock_client._make_request.side_effect = [create_response, workflow_response]

        with patch.object(uploads._uploader, "file_sizes_for_api", return_value=[None]):
            with patch.object(uploads._uploader, "upload_files"):
                uploads.create(upload)

        assert mock_client._make_request.call_count == 2
        workflow_call = mock_client._make_request.call_args_list[1]
        assert workflow_call[1]["method"] == "POST"
        assert workflow_call[1]["endpoint"] == "/uploads/upload-789/start_workflow"

    def test_create_validation_no_source(self, uploads):
        upload = Upload(name="Bad", source="maxquant", filenames=[])

        with pytest.raises(ValueError, match="file_location or s3_bucket"):
            uploads.create(upload)

    def test_create_validation_file_location_without_filenames(self, uploads):
        upload = Upload(
            name="Bad",
            source="maxquant",
            file_location="/tmp",
            filenames=[],
        )

        with pytest.raises(ValueError, match="filenames must be provided"):
            uploads.create(upload)

    @pytest.mark.parametrize(
        "bad_source",
        [
            "raw",
            "diann_raw",
            "generic_format",
            "simple",
            "unknown",
            "diann_matrix",
            "md_diann_maxlfq",
            "msfragger",
        ],
    )
    def test_create_rejects_disallowed_source(self, uploads, bad_source):

        with pytest.raises(
            ValueError, match="1 validation error for Upload\nsource\n  Input should be"
        ):
            upload = Upload(
                name="Bad",
                source=bad_source,
                s3_bucket="bucket",
                filenames=["a.txt"],
                experiment_design=DESIGN,
                sample_metadata=METADATA,
            )
            uploads.create(upload)

    @pytest.mark.parametrize(
        "good_source",
        [
            "maxquant",
            "diann_tabular",
            "tims_diann",
            "spectronaut",
            "md_format",
            "md_format_gene",
        ],
    )
    def test_create_accepts_allowed_sources(self, uploads, mock_client, good_source):
        upload = Upload(
            name="ok",
            source=good_source,
            s3_bucket="bucket",
            filenames=["a.txt"],
            experiment_design=DESIGN,
            sample_metadata=METADATA,
        )
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-ok"}
        mock_client._make_request.return_value = mock_response

        result = uploads.create(upload)

        assert result == "upload-ok"

    def test_create_validation_missing_sample_metadata(self, uploads):
        upload = Upload(
            name="Bad",
            source="maxquant",
            s3_bucket="bucket",
            filenames=["a.txt"],
            experiment_design=DESIGN,
        )

        with pytest.raises(ValueError, match="sample_metadata is required"):
            uploads.create(upload)

    def test_create_failure(self, uploads, mock_client):
        upload = Upload(
            name="Fail",
            source="maxquant",
            s3_bucket="bucket",
            filenames=["a.txt"],
            experiment_design=DESIGN,
            sample_metadata=METADATA,
        )

        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Unprocessable"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to create upload: 422"):
            uploads.create(upload)

    def test_get_by_id_success(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Test Upload",
            "source": "maxquant",
            "status": "completed",
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.get_by_id("upload-1")

        assert isinstance(result, Upload)
        assert result.name == "Test Upload"
        assert result.intensity_dataset is None

        call_args = mock_client._make_request.call_args
        assert call_args[1]["endpoint"] == "/uploads/upload-1"

    def test_get_by_id_parses_intensity_dataset(self, uploads, mock_client):
        dataset_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Test Upload",
            "source": "maxquant",
            "status": "completed",
            "intensity_dataset": {
                "id": dataset_id,
                "name": "Intensity",
                "job_slug": "normalisation_imputation",
                "input_dataset_ids": [],
                "job_run_params": {},
            },
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.get_by_id("upload-1")

        assert isinstance(result.intensity_dataset, Dataset)
        assert str(result.intensity_dataset.id) == dataset_id
        assert result.intensity_dataset.name == "Intensity"

    def test_get_by_id_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get upload: 404"):
            uploads.get_by_id("bad-id")

    def test_delete_success(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 204
        mock_client._make_request.return_value = mock_response

        result = uploads.delete("upload-1")

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "DELETE"
        assert call_args[1]["endpoint"] == "/uploads/upload-1"

    def test_delete_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to delete upload: 404"):
            uploads.delete("upload-1")

    def test_get_sample_metadata_success(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "sample_metadata": [
                ["sample_name", "dose"],
                ["s1", "1"],
            ]
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.get_sample_metadata("upload-1")

        assert isinstance(result, SampleMetadata)
        assert result.data == [["sample_name", "dose"], ["s1", "1"]]

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == "/uploads/upload-1/sample_metadata"

    def test_get_sample_metadata_returns_none_when_missing(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_client._make_request.return_value = mock_response

        result = uploads.get_sample_metadata("upload-1")

        assert result is None

    def test_get_sample_metadata_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get sample metadata: 404"):
            uploads.get_sample_metadata("upload-1")

    def test_query_with_all_filters(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"name": "Upload 1", "source": "maxquant"}],
            "pagination": {"page": 1, "total_pages": 1},
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.query(
            status=["COMPLETED"],
            source=["maxquant"],
            search="test",
            sample_metadata=[{"column": "dose", "value": "1"}],
            page=2,
        )

        assert result["data"][0]["name"] == "Upload 1"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/uploads/query"

        payload = call_args[1]["json"]
        assert payload["status"] == ["COMPLETED"]
        assert payload["source"] == ["maxquant"]
        assert payload["search"] == "test"
        assert payload["sample_metadata"] == [{"column": "dose", "value": "1"}]
        assert payload["page"] == 2

    def test_query_with_defaults(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [], "pagination": {}}
        mock_client._make_request.return_value = mock_response

        uploads.query()

        payload = mock_client._make_request.call_args[1]["json"]
        assert payload == {"page": 1}

    def test_query_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to query uploads: 500"):
            uploads.query()

    def test_update_all_fields(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        result = uploads.update(
            "upload-1",
            name="new name",
            description="new desc",
            experiment_design=DESIGN,
        )

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "PUT"
        assert call_args[1]["endpoint"] == "/uploads/upload-1"
        assert call_args[1]["json"] == {
            "name": "new name",
            "description": "new desc",
            "experiment_design": DESIGN.data,
        }

    def test_update_omits_unset_fields(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        uploads.update("upload-1", name="only name")

        assert mock_client._make_request.call_args[1]["json"] == {"name": "only name"}

    def test_update_empty_description_clears(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        uploads.update("upload-1", description="")

        assert mock_client._make_request.call_args[1]["json"] == {"description": ""}

    def test_update_requires_at_least_one_field(self, uploads, mock_client):
        with pytest.raises(ValueError, match="At least one of name"):
            uploads.update("upload-1")

        mock_client._make_request.assert_not_called()

    def test_update_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "experiment_design can only be updated while draft"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to update upload: 409"):
            uploads.update("upload-1", experiment_design=DESIGN)

    def test_update_sample_metadata_success(self, uploads, mock_client):
        sm = SampleMetadata(data=[["group"], ["a"], ["b"]])

        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        result = uploads.update_sample_metadata("upload-1", sm)

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "PUT"
        assert call_args[1]["endpoint"] == "/uploads/upload-1/sample_metadata"
        assert call_args[1]["json"] == {"sample_metadata": sm.data}

    def test_update_sample_metadata_failure(self, uploads, mock_client):
        sm = SampleMetadata(data=[["group"], ["a"]])

        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Invalid"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to update sample metadata: 422"):
            uploads.update_sample_metadata("upload-1", sm)

    def test_wait_until_complete_success(self, uploads, mock_client, mocker):
        upload = Upload(
            name="x",
            source=Source.diann_tabular,
            s3_bucket="b",
            filenames=[],
            status="completed",
        )
        mocker.patch.object(uploads, "get_by_id", return_value=upload)

        result = uploads.wait_until_complete("upload-1", poll_s=0, timeout_s=1)

        assert isinstance(result, Upload)

    def test_wait_until_complete_failure(self, uploads, mock_client, mocker):
        upload = Upload(
            name="x",
            source=Source.diann_tabular,
            s3_bucket="b",
            filenames=[],
            status="processing_failed",
        )
        mocker.patch.object(uploads, "get_by_id", return_value=upload)

        with pytest.raises(Exception, match="failed"):
            uploads.wait_until_complete("upload-1", poll_s=0, timeout_s=1)

    def test_wait_until_complete_draft_fails_fast(self, uploads, mock_client, mocker):
        upload = Upload(
            name="x",
            source=Source.diann_tabular,
            s3_bucket="b",
            filenames=[],
            status="draft",
        )
        get_by_id = mocker.patch.object(uploads, "get_by_id", return_value=upload)

        with pytest.raises(Exception, match="draft status"):
            uploads.wait_until_complete("upload-1", poll_s=0, timeout_s=60)

        # Fails fast on the first poll rather than looping until timeout.
        assert get_by_id.call_count == 1

    def test_uploader_uses_uploads_resource_path(self, uploads):
        assert uploads._uploader._resource_path == "/uploads"


class TestV2UploadAgentRun:
    RUN_ID = "0b7e6c2a-3f1d-4b8e-9a6c-1d2e3f4a5b6c"

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def uploads(self, mock_client):
        return Uploads(mock_client)

    def _respond(self, mock_client, status_code, body=None, text=""):
        response = Mock()
        response.status_code = status_code
        response.json.return_value = body
        response.text = text
        mock_client._make_request.return_value = response

    def test_posts_run_and_returns_result(self, uploads, mock_client):
        result = {
            "id": self.RUN_ID,
            "status": "succeeded",
            "upload_id": "upload-1",
            "source": "diann_tabular",
            "failure_reason": None,
            "message": "Started.",
        }
        self._respond(mock_client, 200, result)

        out = uploads.run_upload_agent(
            id=self.RUN_ID,
            name="My dataset",
            filenames=["report.pg_matrix.tsv"],
            description="Some notes",
        )

        assert out == result
        kwargs = mock_client._make_request.call_args[1]
        assert kwargs["method"] == "POST"
        assert kwargs["endpoint"] == "/uploads/upload_agent_run"
        assert kwargs["json"] == {
            "id": self.RUN_ID,
            "name": "My dataset",
            "filenames": ["report.pg_matrix.tsv"],
            "description": "Some notes",
        }
        assert kwargs["timeout"] > 15 * 60

    def test_omits_description_when_none(self, uploads, mock_client):
        self._respond(mock_client, 200, {"status": "succeeded"})

        uploads.run_upload_agent(id=self.RUN_ID, name="n", filenames=["a.tsv"])

        assert "description" not in mock_client._make_request.call_args[1]["json"]

    def test_failed_run_is_returned_not_raised(self, uploads, mock_client):
        failed = {"status": "failed", "failure_reason": "unsupported_format"}
        self._respond(mock_client, 200, failed)

        assert (
            uploads.run_upload_agent(id=self.RUN_ID, name="n", filenames=["a"])
            == failed
        )

    @pytest.mark.parametrize("status_code", [400, 404, 422, 503])
    def test_error_status_raises(self, uploads, mock_client, status_code):
        self._respond(mock_client, status_code, text="nope")

        with pytest.raises(Exception, match=f"{status_code} - nope"):
            uploads.run_upload_agent(id=self.RUN_ID, name="n", filenames=["a"])

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"id": "not-a-uuid"},
            {"name": "  "},
            {"filenames": []},
            {"filenames": ["a", "a"]},
            {"filenames": ["dir/a"]},
            {"filenames": ["..a"]},
            {"filenames": ["a\\b"]},
            {"filenames": ["."]},
        ],
    )
    def test_invalid_input_raises_without_request(self, uploads, mock_client, kwargs):
        args = {"id": self.RUN_ID, "name": "n", "filenames": ["a"], **kwargs}

        with pytest.raises(ValueError):
            uploads.run_upload_agent(**args)

        mock_client._make_request.assert_not_called()
