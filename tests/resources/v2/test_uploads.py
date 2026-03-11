from unittest.mock import Mock, patch

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import SampleMetadata, Upload
from md_python.resources.v2.uploads import Uploads


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

    def test_create_with_file_location(self, uploads, mock_client):
        upload = Upload(
            name="Local Upload",
            source="maxquant",
            file_location="/tmp/files",
            filenames=["data.raw"],
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "upload-456"}
        mock_client._make_request.return_value = mock_response

        with patch.object(uploads._uploader, "file_sizes_for_api", return_value=[None]):
            with patch.object(uploads._uploader, "upload_files"):
                result = uploads.create(upload)

        assert result == "upload-456"

    def test_create_with_file_upload_triggers_workflow(self, uploads, mock_client):
        upload = Upload(
            name="Upload With Files",
            source="maxquant",
            file_location="/tmp/files",
            filenames=["data.raw"],
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

    def test_create_failure(self, uploads, mock_client):
        upload = Upload(
            name="Fail",
            source="maxquant",
            s3_bucket="bucket",
            filenames=["a.txt"],
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
            "status": "COMPLETED",
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.get_by_id("upload-1")

        assert isinstance(result, Upload)
        assert result.name == "Test Upload"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["endpoint"] == "/uploads/upload-1"

    def test_get_by_id_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get upload: 404"):
            uploads.get_by_id("bad-id")

    def test_get_by_name_success(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "Named Upload",
            "source": "maxquant",
        }
        mock_client._make_request.return_value = mock_response

        result = uploads.get_by_name("Named Upload")

        assert isinstance(result, Upload)
        assert result.name == "Named Upload"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["endpoint"] == "/uploads?name=Named Upload"

    def test_get_by_name_failure(self, uploads, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get upload by name: 404"):
            uploads.get_by_name("nope")

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
        upload = Upload(name="x", source="s", s3_bucket="b", filenames=[], status="COMPLETED")
        mocker.patch.object(uploads, "get_by_id", return_value=upload)

        result = uploads.wait_until_complete("upload-1", poll_s=0, timeout_s=1)

        assert isinstance(result, Upload)

    def test_wait_until_complete_failure(self, uploads, mock_client, mocker):
        upload = Upload(name="x", source="s", s3_bucket="b", filenames=[], status="FAILED")
        mocker.patch.object(uploads, "get_by_id", return_value=upload)

        with pytest.raises(Exception, match="failed"):
            uploads.wait_until_complete("upload-1", poll_s=0, timeout_s=1)

    def test_uploader_uses_uploads_resource_path(self, uploads):
        assert uploads._uploader._resource_path == "/uploads"
