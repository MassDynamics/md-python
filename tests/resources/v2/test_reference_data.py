from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import ReferenceDataFile
from md_python.resources.v2.reference_data import ReferenceData

# id is "<org_uuid>/<file_uuid>"
REF_ID = "11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222"


class TestV2ReferenceData:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def reference_data(self, mock_client):
        return ReferenceData(mock_client)

    def test_create_upload_single(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "reference_data": {"id": REF_ID, "filename": "genes.csv"},
            "upload": {
                "filename": "genes.csv",
                "url": "https://s3/put",
                "mode": "single",
            },
        }
        mock_client._make_request.return_value = mock_response

        result = reference_data.create_upload("genes.csv", 0)

        assert result["reference_data"] == ReferenceDataFile(
            id=REF_ID, filename="genes.csv"
        )
        assert result["upload"] == {
            "filename": "genes.csv",
            "url": "https://s3/put",
            "mode": "single",
        }

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/reference_data"
        assert call_args[1]["json"] == {"filename": "genes.csv", "file_size": 0}

    def test_create_upload_multipart_sends_file_size(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "reference_data": {"id": REF_ID, "filename": "big.parquet"},
            "upload": {"mode": "multipart"},
        }
        mock_client._make_request.return_value = mock_response

        reference_data.create_upload("big.parquet", 123456789)

        payload = mock_client._make_request.call_args[1]["json"]
        assert payload == {"filename": "big.parquet", "file_size": 123456789}

    def test_create_upload_failure(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Invalid filename"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(
            Exception, match="Failed to create reference data upload: 400"
        ):
            reference_data.create_upload("bad", 0)

    def test_complete_upload_success(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_client._make_request.return_value = mock_response

        result = reference_data.complete_upload(REF_ID, "big.parquet", "session-abc")

        assert result is True
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/reference_data/complete"
        assert call_args[1]["json"] == {
            "reference_data": {"id": REF_ID, "filename": "big.parquet"},
            "upload_session_id": "session-abc",
        }

    def test_complete_upload_failure(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Upload completion failed"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(
            Exception, match="Failed to complete reference data upload: 422"
        ):
            reference_data.complete_upload(REF_ID, "big.parquet", "s")

    def test_list_success(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": REF_ID, "filename": "genes.csv"},
        ]
        mock_client._make_request.return_value = mock_response

        result = reference_data.list()

        assert result == [ReferenceDataFile(id=REF_ID, filename="genes.csv")]
        assert isinstance(result[0], ReferenceDataFile)
        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == "/reference_data"

    def test_list_failure(self, reference_data, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Server error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to list reference data: 500"):
            reference_data.list()

    def test_upload_single(self, reference_data, mocker):
        """Small files send file_size=0, take a single PUT and no completion."""
        mocker.patch.object(reference_data._uploader, "_validate_file_exists")
        mocker.patch.object(
            reference_data._uploader, "_get_file_size", return_value=1024
        )
        mocker.patch.object(
            reference_data._uploader, "should_use_multipart", return_value=False
        )
        create = mocker.patch.object(
            reference_data,
            "create_upload",
            return_value={
                "reference_data": ReferenceDataFile(id=REF_ID, filename="genes.csv"),
                "upload": {"url": "https://s3/put", "mode": "single"},
            },
        )
        single = mocker.patch.object(reference_data._uploader, "upload_single_file")
        complete = mocker.patch.object(reference_data, "complete_upload")

        result = reference_data.upload("/tmp/genes.csv")

        assert result == ReferenceDataFile(id=REF_ID, filename="genes.csv")
        create.assert_called_once_with("genes.csv", 0)
        single.assert_called_once_with("https://s3/put", "/tmp/genes.csv", "genes.csv")
        complete.assert_not_called()

    def test_upload_multipart(self, reference_data, mocker):
        """Large files send the real size, transfer parts, then complete."""
        mocker.patch.object(reference_data._uploader, "_validate_file_exists")
        mocker.patch.object(
            reference_data._uploader, "_get_file_size", return_value=104857600
        )
        mocker.patch.object(
            reference_data._uploader, "should_use_multipart", return_value=True
        )
        parts = [{"url": "https://s3/part1", "part_number": 1}]
        create = mocker.patch.object(
            reference_data,
            "create_upload",
            return_value={
                "reference_data": ReferenceDataFile(id=REF_ID, filename="ref.parquet"),
                "upload": {
                    "mode": "multipart",
                    "upload_session_id": "session-xyz",
                    "parts": parts,
                },
            },
        )
        multipart = mocker.patch.object(
            reference_data._uploader, "upload_multipart_file"
        )
        complete = mocker.patch.object(reference_data, "complete_upload")

        result = reference_data.upload("/tmp/big.parquet", filename="ref.parquet")

        assert result == ReferenceDataFile(id=REF_ID, filename="ref.parquet")
        create.assert_called_once_with("ref.parquet", 104857600)
        multipart.assert_called_once_with(parts, "/tmp/big.parquet", "ref.parquet")
        complete.assert_called_once_with(REF_ID, "ref.parquet", "session-xyz")

    def test_upload_missing_file(self, reference_data, mocker):
        mocker.patch.object(
            reference_data._uploader,
            "_validate_file_exists",
            side_effect=FileNotFoundError("File not found: /tmp/nope.csv"),
        )

        with pytest.raises(FileNotFoundError, match="File not found"):
            reference_data.upload("/tmp/nope.csv")

    def test_uploader_uses_reference_data_resource_path(self, reference_data):
        assert reference_data._uploader._resource_path == "/reference_data"


def test_client_exposes_reference_data():
    client = MDClientV2(api_token="t", base_url="http://localhost/api")
    assert isinstance(client.reference_data, ReferenceData)
