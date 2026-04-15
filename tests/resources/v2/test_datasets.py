from unittest.mock import Mock
from uuid import UUID

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import Dataset
from md_python.resources.v2.datasets import Datasets


class TestV2Datasets:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

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

    def test_download_table_url_failure(self, datasets, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to get download URL: 404"):
            datasets.download_table_url("ds-1", "intensity")

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
        mocker.patch.object(datasets, "list_by_upload", return_value=[completed_ds])

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
        mocker.patch.object(datasets, "list_by_upload", return_value=[failed_ds])

        with pytest.raises(Exception, match="failed"):
            datasets.wait_until_complete(
                "upload-1",
                "11111111-1111-1111-1111-111111111111",
                poll_s=0,
                timeout_s=1,
            )

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
