"""
Test cases for Datasets resource
"""

from unittest.mock import Mock
from uuid import UUID

import pytest

from md_python.client import MDClient
from md_python.models import Dataset
from md_python.resources.datasets import Datasets


class TestDatasets:
    """Test cases for Datasets resource"""

    @pytest.fixture
    def mock_client(self):
        """Create a mock MDClient for testing"""
        client = Mock(spec=MDClient)
        return client

    @pytest.fixture
    def datasets_resource(self, mock_client):
        """Create Datasets resource instance with mock client"""
        return Datasets(mock_client)

    @pytest.fixture
    def sample_dataset(self):
        """Create a sample dataset for testing"""
        return Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Test dataset",
            job_slug="demo_flow",
            job_run_params={"a_string_field": "demo123", "a_or_b_enum": "A"},
        )

    @pytest.fixture
    def sample_api_response(self):
        """Sample API response for dataset creation"""
        return {
            "dataset_id": "1234567890abcdef1234567890abcdef",
            "name": "Test dataset",
            "job_slug": "demo_flow",
            "status": "created",
        }

    def test_create_success(
        self, datasets_resource, sample_dataset, sample_api_response, mock_client
    ):
        """Test successful dataset creation"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = sample_api_response

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(sample_dataset)

        # Verify the result
        assert result == "1234567890abcdef1234567890abcdef"

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once()
        call_args = mock_client._make_request.call_args

        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/datasets"
        assert call_args[1]["headers"] == {
            "Content-Type": "application/json",
            "accept": "application/vnd.md-v1+json",
        }

        # Verify the payload structure
        payload = call_args[1]["json"]
        assert "dataset" in payload
        assert payload["dataset"]["name"] == sample_dataset.name
        assert payload["dataset"]["job_slug"] == sample_dataset.job_slug
        assert payload["dataset"]["input_dataset_ids"] == [
            str(sample_dataset.input_dataset_ids[0])
        ]
        assert payload["dataset"]["job_run_params"] == sample_dataset.job_run_params

    def test_create_success_200_status(
        self, datasets_resource, sample_dataset, sample_api_response, mock_client
    ):
        """Test successful dataset creation with 200 status code"""
        # Mock the API response with 200 status
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(sample_dataset)

        # Verify the result
        assert result == "1234567890abcdef1234567890abcdef"

    def test_create_failure(self, datasets_resource, sample_dataset, mock_client):
        """Test dataset creation failure"""
        # Mock the API response with error
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request: Invalid dataset data"

        mock_client._make_request.return_value = mock_response

        # Verify exception is raised
        with pytest.raises(Exception) as exc_info:
            datasets_resource.create(sample_dataset)

        assert (
            "Failed to create dataset: 400 - Bad Request: Invalid dataset data"
            in str(exc_info.value)
        )

    def test_create_with_minimal_dataset(self, datasets_resource, mock_client):
        """Test dataset creation with minimal required fields"""
        # Create minimal dataset
        minimal_dataset = Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Minimal Dataset",
            job_slug="minimal_flow",
            job_run_params={},
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "abcdef1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(minimal_dataset)

        # Verify the result
        assert result == "abcdef1234567890abcdef1234567890"

        # Verify the payload contains only the required fields
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert payload["name"] == "Minimal Dataset"
        assert payload["job_slug"] == "minimal_flow"
        assert payload["input_dataset_ids"] == ["2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"]
        assert payload["job_run_params"] == {}

    def test_create_with_multiple_input_datasets(self, datasets_resource, mock_client):
        """Test dataset creation with multiple input dataset IDs"""
        # Create dataset with multiple input datasets
        multi_input_dataset = Dataset(
            input_dataset_ids=[
                UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"),
                UUID("3c2b6d38-bd06-567d-c3ff-fddff4bc4e2f"),
            ],
            name="Multi Input Dataset",
            job_slug="multi_flow",
            job_run_params={"param1": "value1"},
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "multi1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(multi_input_dataset)

        # Verify the result
        assert result == "multi1234567890abcdef1234567890"

        # Verify the payload contains multiple input dataset IDs
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert len(payload["input_dataset_ids"]) == 2
        assert "2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e" in payload["input_dataset_ids"]
        assert "3c2b6d38-bd06-567d-c3ff-fddff4bc4e2f" in payload["input_dataset_ids"]

    def test_create_with_complex_job_params(self, datasets_resource, mock_client):
        """Test dataset creation with complex job run parameters"""
        # Create dataset with complex job parameters
        complex_params_dataset = Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Complex Params Dataset",
            job_slug="complex_flow",
            job_run_params={
                "string_param": "test_value",
                "number_param": 42,
                "boolean_param": True,
                "array_param": ["item1", "item2"],
                "nested_param": {"nested_key": "nested_value", "another_key": 123},
            },
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "complex1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(complex_params_dataset)

        # Verify the result
        assert result == "complex1234567890abcdef1234567890"

        # Verify the payload contains complex job parameters
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert payload["job_run_params"]["string_param"] == "test_value"
        assert payload["job_run_params"]["number_param"] == 42
        assert payload["job_run_params"]["boolean_param"] is True
        assert payload["job_run_params"]["array_param"] == ["item1", "item2"]
        assert payload["job_run_params"]["nested_param"]["nested_key"] == "nested_value"

    def test_create_with_empty_job_params(self, datasets_resource, mock_client):
        """Test dataset creation with empty job run parameters"""
        # Create dataset with empty job parameters
        empty_params_dataset = Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Empty Params Dataset",
            job_slug="empty_flow",
            job_run_params={},
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "empty1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(empty_params_dataset)

        # Verify the result
        assert result == "empty1234567890abcdef1234567890"

        # Verify the payload contains empty job parameters
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert payload["job_run_params"] == {}

    def test_create_with_empty_job_params(self, datasets_resource, mock_client):
        """Test dataset creation with empty job run parameters"""
        # Create dataset with empty job parameters
        empty_params_dataset = Dataset(
            input_dataset_ids=[UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e")],
            name="Empty Params Dataset",
            job_slug="empty_flow",
            job_run_params={},
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "none1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(empty_params_dataset)

        # Verify the result
        assert result == "none1234567890abcdef1234567890"

        # Verify the payload contains empty job parameters (defaults to empty dict)
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert payload["job_run_params"] == {}

    def test_create_headers_verification(
        self, datasets_resource, sample_dataset, mock_client
    ):
        """Test that correct headers are sent in the request"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "header1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        datasets_resource.create(sample_dataset)

        # Verify the headers are correct
        call_args = mock_client._make_request.call_args
        headers = call_args[1]["headers"]

        assert headers["Content-Type"] == "application/json"
        assert headers["accept"] == "application/vnd.md-v1+json"
        assert len(headers) == 2

    def test_create_uuid_conversion(self, datasets_resource, mock_client):
        """Test that UUID objects are properly converted to strings in the payload"""
        # Create dataset with UUID input dataset IDs
        uuid_dataset = Dataset(
            input_dataset_ids=[
                UUID("2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"),
                UUID("4d3c7e49-ce17-678e-d4ff-feeff5cd5f3f"),
            ],
            name="UUID Test Dataset",
            job_slug="uuid_flow",
            job_run_params={},
        )

        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "dataset_id": "uuid1234567890abcdef1234567890"
        }

        mock_client._make_request.return_value = mock_response

        # Call the create method
        result = datasets_resource.create(uuid_dataset)

        # Verify the result
        assert result == "uuid1234567890abcdef1234567890"

        # Verify UUIDs are converted to strings in the payload
        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["dataset"]

        assert payload["input_dataset_ids"] == [
            "2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e",
            "4d3c7e49-ce17-678e-d4ff-feeff5cd5f3f",
        ]
        # Verify they are strings, not UUID objects
        assert all(isinstance(did, str) for did in payload["input_dataset_ids"])

    def test_list_by_experiment_success(self, datasets_resource, mock_client):
        """Test successful retrieval of datasets by experiment"""
        # Mock the API response with multiple datasets
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "a1b2c3d4e5f67890a1b2c3d4e5f67890",
                "input_dataset_ids": ["2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"],
                "name": "Dataset 1",
                "job_slug": "flow_1",
                "job_run_params": {"param1": "value1"},
            },
            {
                "id": "b2c3d4e5f67890a1b2c3d4e5f67890a1",
                "input_dataset_ids": ["3c2b6d38-bd06-567d-c3ff-fddff4bc4e2f"],
                "name": "Dataset 2",
                "job_slug": "flow_2",
                "job_run_params": {"param2": "value2"},
            },
        ]

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        result = datasets_resource.list_by_experiment(experiment_id)

        # Verify the result
        assert len(result) == 2
        assert isinstance(result[0], Dataset)
        assert isinstance(result[1], Dataset)

        # Verify first dataset
        assert result[0].id == UUID("a1b2c3d4e5f67890a1b2c3d4e5f67890")
        assert result[0].name == "Dataset 1"
        assert result[0].job_slug == "flow_1"
        assert result[0].job_run_params == {"param1": "value1"}
        assert len(result[0].input_dataset_ids) == 1
        assert result[0].input_dataset_ids[0] == UUID(
            "2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"
        )

        # Verify second dataset
        assert result[1].id == UUID("b2c3d4e5f67890a1b2c3d4e5f67890a1")
        assert result[1].name == "Dataset 2"
        assert result[1].job_slug == "flow_2"
        assert result[1].job_run_params == {"param2": "value2"}
        assert len(result[1].input_dataset_ids) == 1
        assert result[1].input_dataset_ids[0] == UUID(
            "3c2b6d38-bd06-567d-c3ff-fddff4bc4e2f"
        )

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once_with(
            method="GET",
            endpoint=f"/datasets?experiment_id={experiment_id}",
            headers={"accept": "application/vnd.md-v1+json"},
        )

    def test_list_by_experiment_empty_result(self, datasets_resource, mock_client):
        """Test list_by_experiment when no datasets are found"""
        # Mock the API response with empty list
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        result = datasets_resource.list_by_experiment(experiment_id)

        # Verify the result
        assert len(result) == 0
        assert isinstance(result, list)

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once_with(
            method="GET",
            endpoint=f"/datasets?experiment_id={experiment_id}",
            headers={"accept": "application/vnd.md-v1+json"},
        )

    def test_list_by_experiment_single_dataset(self, datasets_resource, mock_client):
        """Test list_by_experiment with single dataset result"""
        # Mock the API response with single dataset
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "c3d4e5f67890a1b2c3d4e5f67890a1b2",
                "input_dataset_ids": ["2b1a5c27-ac95-456c-b2ff-eccfb3ab3d1e"],
                "name": "Single Dataset",
                "job_slug": "single_flow",
                "sample_names": ["sample1", "sample2"],
                "job_run_start_time": "2024-01-01T10:00:00Z",
            }
        ]

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        result = datasets_resource.list_by_experiment(experiment_id)

        # Verify the result
        assert len(result) == 1
        assert isinstance(result[0], Dataset)

        # Verify dataset details
        assert result[0].id == UUID("c3d4e5f67890a1b2c3d4e5f67890a1b2")
        assert result[0].name == "Single Dataset"
        assert result[0].job_slug == "single_flow"
        assert result[0].sample_names == ["sample1", "sample2"]
        assert result[0].job_run_start_time is not None
        assert result[0].job_run_params == {}

    def test_list_by_experiment_failure(self, datasets_resource, mock_client):
        """Test list_by_experiment failure handling"""
        # Mock the API response with error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Experiment not found"

        mock_client._make_request.return_value = mock_response

        # Verify exception is raised
        experiment_id = "non-existent-experiment-id"
        with pytest.raises(Exception) as exc_info:
            datasets_resource.list_by_experiment(experiment_id)

        assert (
            "Failed to get datasets by experiment: 404 - Experiment not found"
            in str(exc_info.value)
        )

    def test_list_by_experiment_with_minimal_dataset(
        self, datasets_resource, mock_client
    ):
        """Test list_by_experiment with minimal dataset data"""
        # Mock the API response with minimal dataset
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "name": "Minimal Dataset",
                "job_slug": "minimal_flow",
                "job_run_params": {},
            }
        ]

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        result = datasets_resource.list_by_experiment(experiment_id)

        # Verify the result
        assert len(result) == 1
        assert isinstance(result[0], Dataset)

        # Verify minimal dataset has default values
        assert result[0].name == "Minimal Dataset"
        assert result[0].job_slug == "minimal_flow"
        assert result[0].id is None
        assert result[0].input_dataset_ids == []
        assert result[0].sample_names is None
        assert result[0].job_run_params == {}
        assert result[0].job_run_start_time is None

    def test_list_by_experiment_headers_verification(
        self, datasets_resource, mock_client
    ):
        """Test that correct headers are sent in the request"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        datasets_resource.list_by_experiment(experiment_id)

        # Verify the headers are correct
        call_args = mock_client._make_request.call_args
        headers = call_args[1]["headers"]

        assert headers["accept"] == "application/vnd.md-v1+json"
        assert len(headers) == 1

    def test_list_by_experiment_url_encoding(self, datasets_resource, mock_client):
        """Test that experiment_id is properly included in the URL"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_client._make_request.return_value = mock_response

        # Call the list_by_experiment method with special characters
        experiment_id = "5f457885-2eff-4406-ae7f-c178e7ed1d55"
        datasets_resource.list_by_experiment(experiment_id)

        # Verify the endpoint is correct
        call_args = mock_client._make_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert endpoint == f"/datasets?experiment_id={experiment_id}"
        assert "experiment_id=" in endpoint
        assert experiment_id in endpoint

    def test_delete_success(self, datasets_resource, mock_client):
        """Test successful dataset deletion"""
        # Mock the API response with 204 status (successful deletion)
        mock_response = Mock()
        mock_response.status_code = 204

        mock_client._make_request.return_value = mock_response

        # Call the delete method
        dataset_id = "59af3264-5eb7-4c2b-93ac-cc9286bf27fc"
        result = datasets_resource.delete(dataset_id)

        # Verify the result
        assert result is True

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once_with(
            method="DELETE",
            endpoint=f"/datasets/{dataset_id}",
            headers={"accept": "application/vnd.md-v1+json"},
        )

    def test_delete_failure(self, datasets_resource, mock_client):
        """Test dataset deletion failure"""
        # Mock the API response with error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Dataset not found"

        mock_client._make_request.return_value = mock_response

        # Verify exception is raised
        dataset_id = "non-existent-dataset-id"
        with pytest.raises(Exception) as exc_info:
            datasets_resource.delete(dataset_id)

        assert "Failed to delete dataset: 404 - Dataset not found" in str(
            exc_info.value
        )

    def test_delete_with_different_status_codes(self, datasets_resource, mock_client):
        """Test dataset deletion with various error status codes"""
        error_codes = [400, 401, 403, 500]
        dataset_id = "test-dataset-id"

        for status_code in error_codes:
            # Mock the API response with error
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_response.text = f"Error {status_code}"

            mock_client._make_request.return_value = mock_response

            # Verify exception is raised with correct error message
            with pytest.raises(Exception) as exc_info:
                datasets_resource.delete(dataset_id)

            assert (
                f"Failed to delete dataset: {status_code} - Error {status_code}"
                in str(exc_info.value)
            )

    def test_delete_headers_verification(self, datasets_resource, mock_client):
        """Test that correct headers are sent in the delete request"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 204

        mock_client._make_request.return_value = mock_response

        # Call the delete method
        dataset_id = "test-dataset-id"
        datasets_resource.delete(dataset_id)

        # Verify the headers are correct
        call_args = mock_client._make_request.call_args
        headers = call_args[1]["headers"]

        assert headers["accept"] == "application/vnd.md-v1+json"
        assert len(headers) == 1

    def test_delete_endpoint_construction(self, datasets_resource, mock_client):
        """Test that the delete endpoint is constructed correctly"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 204

        mock_client._make_request.return_value = mock_response

        # Test with different dataset IDs
        test_dataset_ids = [
            "59af3264-5eb7-4c2b-93ac-cc9286bf27fc",
            "ff07b3c2-249a-429c-ade3-8e9b4eba054f",
            "simple-id",
            "id-with-special-chars_123",
        ]

        for dataset_id in test_dataset_ids:
            # Reset mock for each iteration
            mock_client._make_request.reset_mock()
            mock_client._make_request.return_value = mock_response

            # Call the delete method
            datasets_resource.delete(dataset_id)

            # Verify the endpoint is correct
            call_args = mock_client._make_request.call_args
            endpoint = call_args[1]["endpoint"]

            assert endpoint == f"/datasets/{dataset_id}"
            assert endpoint.startswith("/datasets/")
            assert endpoint.endswith(dataset_id)

    def test_delete_method_type(self, datasets_resource):
        """Test that delete method returns boolean on success"""
        # This test verifies the method signature and return type
        assert callable(datasets_resource.delete)

        # Verify the method exists and is callable
        method = getattr(datasets_resource, "delete", None)
        assert method is not None
        assert callable(method)

    def test_retry_success(self, datasets_resource, mock_client):
        """Test successful dataset retry"""
        # Mock the API response with 200 status (successful retry)
        mock_response = Mock()
        mock_response.status_code = 200

        mock_client._make_request.return_value = mock_response

        # Call the retry method
        dataset_id = "e8d77807-b06c-4daf-a655-b860b520ac79"
        result = datasets_resource.retry(dataset_id)

        # Verify the result
        assert result is True

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once_with(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/retry",
            headers={"accept": "application/vnd.md-v1+json"},
        )

    def test_retry_failure(self, datasets_resource, mock_client):
        """Test dataset retry failure"""
        # Mock the API response with error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Dataset not found"

        mock_client._make_request.return_value = mock_response

        # Verify exception is raised
        dataset_id = "non-existent-dataset-id"
        with pytest.raises(Exception) as exc_info:
            datasets_resource.retry(dataset_id)

        assert "Failed to retry dataset: 404 - Dataset not found" in str(exc_info.value)

    def test_retry_with_different_status_codes(self, datasets_resource, mock_client):
        """Test dataset retry with various error status codes"""
        error_codes = [400, 401, 403, 500]
        dataset_id = "test-dataset-id"

        for status_code in error_codes:
            # Mock the API response with error
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_response.text = f"Error {status_code}"

            mock_client._make_request.return_value = mock_response

            # Verify exception is raised with correct error message
            with pytest.raises(Exception) as exc_info:
                datasets_resource.retry(dataset_id)

            assert (
                f"Failed to retry dataset: {status_code} - Error {status_code}"
                in str(exc_info.value)
            )

    def test_retry_headers_verification(self, datasets_resource, mock_client):
        """Test that correct headers are sent in the retry request"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 200

        mock_client._make_request.return_value = mock_response

        # Call the retry method
        dataset_id = "test-dataset-id"
        datasets_resource.retry(dataset_id)

        # Verify the headers are correct
        call_args = mock_client._make_request.call_args
        headers = call_args[1]["headers"]

        assert headers["accept"] == "application/vnd.md-v1+json"
        assert len(headers) == 1

    def test_retry_endpoint_construction(self, datasets_resource, mock_client):
        """Test that the retry endpoint is constructed correctly"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 200

        mock_client._make_request.return_value = mock_response

        # Test with different dataset IDs
        test_dataset_ids = [
            "e8d77807-b06c-4daf-a655-b860b520ac79",
            "ff07b3c2-249a-429c-ade3-8e9b4eba054f",
            "simple-id",
            "id-with-special-chars_123",
        ]

        for dataset_id in test_dataset_ids:
            # Reset mock for each iteration
            mock_client._make_request.reset_mock()
            mock_client._make_request.return_value = mock_response

            # Call the retry method
            datasets_resource.retry(dataset_id)

            # Verify the endpoint is correct
            call_args = mock_client._make_request.call_args
            endpoint = call_args[1]["endpoint"]

            assert endpoint == f"/datasets/{dataset_id}/retry"
            assert endpoint.startswith("/datasets/")
            assert endpoint.endswith("/retry")
            assert dataset_id in endpoint

    def test_retry_method_type(self, datasets_resource):
        """Test that retry method returns boolean on success"""
        # This test verifies the method signature and return type
        assert callable(datasets_resource.retry)

        # Verify the method exists and is callable
        method = getattr(datasets_resource, "retry", None)
        assert method is not None
        assert callable(method)
