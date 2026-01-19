from unittest.mock import Mock, mock_open, patch

import pytest

from md_python.client import MDClient
from md_python.models import Experiment, ExperimentDesign, SampleMetadata
from md_python.resources.experiments import Experiments


class TestExperiments:
    """Test cases for Experiments resource"""

    @pytest.fixture
    def mock_client(self):
        """Create a mock MDClient for testing"""
        client = Mock(spec=MDClient)
        return client

    @pytest.fixture
    def experiments_resource(self, mock_client):
        """Create Experiments resource instance with mock client"""
        return Experiments(mock_client)

    @pytest.fixture
    def sample_experiment(self):
        """Create a sample experiment for testing"""
        experiment_design = ExperimentDesign(
            data=[["condition", "replicate"], ["control", "1"], ["treatment", "1"]]
        )
        sample_metadata = SampleMetadata(
            data=[
                ["sample", "condition"],
                ["sample1", "control"],
                ["sample2", "treatment"],
            ]
        )

        return Experiment(
            name="Test Experiment",
            description="A test experiment for unit testing",
            experiment_design=experiment_design,
            labelling_method="manual",
            source="test_source",
            s3_bucket="test-bucket",
            s3_prefix="experiments/test/",
            filenames=["file1.txt", "file2.txt"],
            sample_metadata=sample_metadata,
        )

    @pytest.fixture
    def sample_api_response(self):
        """Sample API response for experiment creation"""
        return {
            "id": "1234567890abcdef1234567890abcdef",
            "name": "Test Experiment",
            "description": "A test experiment for unit testing",
            "status": "created",
        }

    @pytest.fixture
    def sample_experiment_response(self):
        """Sample API response for getting experiment by ID"""
        return {
            "id": "1234567890abcdef1234567890abcdef",
            "name": "Test Experiment",
            "description": "A test experiment for unit testing",
            "experiment_design": [
                ["condition", "replicate"],
                ["control", "1"],
                ["treatment", "1"],
            ],
            "labelling_method": "manual",
            "source": "test_source",
            "s3_bucket": "test-bucket",
            "s3_prefix": "experiments/test/",
            "filenames": ["file1.txt", "file2.txt"],
            "sample_metadata": [
                ["sample", "condition"],
                ["sample1", "control"],
                ["sample2", "treatment"],
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "status": "active",
        }

    def test_create_success(
        self, experiments_resource, sample_experiment, sample_api_response, mock_client
    ):
        """Test successful experiment creation"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = sample_api_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.create(sample_experiment)

        assert result == "1234567890abcdef1234567890abcdef"

        mock_client._make_request.assert_called_once()
        call_args = mock_client._make_request.call_args

        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/experiments"
        assert call_args[1]["headers"] == {"Content-Type": "application/json"}

        payload = call_args[1]["json"]
        assert "experiment" in payload
        assert payload["experiment"]["name"] == sample_experiment.name
        assert payload["experiment"]["description"] == sample_experiment.description
        assert payload["experiment"]["source"] == sample_experiment.source
        assert payload["experiment"]["s3_bucket"] == sample_experiment.s3_bucket
        assert payload["experiment"]["s3_prefix"] == sample_experiment.s3_prefix
        assert payload["experiment"]["filenames"] == sample_experiment.filenames
        assert (
            payload["experiment"]["experiment_design"]
            == sample_experiment.experiment_design.data
        )
        assert (
            payload["experiment"]["sample_metadata"]
            == sample_experiment.sample_metadata.data
        )

    def test_create_success_200_status(
        self, experiments_resource, sample_experiment, sample_api_response, mock_client
    ):
        """Test successful experiment creation with 200 status code"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.create(sample_experiment)

        assert result == "1234567890abcdef1234567890abcdef"

    def test_create_failure(self, experiments_resource, sample_experiment, mock_client):
        """Test experiment creation failure"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request: Invalid experiment data"

        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception) as exc_info:
            experiments_resource.create(sample_experiment)

        assert (
            "Failed to create experiment: 400 - Bad Request: Invalid experiment data"
            in str(exc_info.value)
        )

    def test_create_with_minimal_experiment(self, experiments_resource, mock_client):
        """Test experiment creation with minimal required fields"""
        minimal_experiment = Experiment(
            name="Minimal Experiment",
            source="minimal_source",
            s3_bucket="minimal-bucket",
            filenames=[],
        )

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "abcdef1234567890abcdef1234567890"}

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.create(minimal_experiment)

        assert result == "abcdef1234567890abcdef1234567890"

        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]["experiment"]

        assert payload["name"] == "Minimal Experiment"
        assert payload["source"] == "minimal_source"
        assert payload["description"] is None
        assert payload["experiment_design"] is None
        assert payload["sample_metadata"] is None

    @patch("md_python.uploads.requests.put")
    @patch("md_python.uploads.os.path.getsize")
    @patch("md_python.uploads.os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data=b"file content")
    def test_create_with_file_location_and_uploads(
        self,
        mock_file,
        mock_exists,
        mock_getsize,
        mock_requests_put,
        experiments_resource,
        mock_client,
    ):
        experiment = Experiment(
            name="File Upload Experiment",
            source="test_source",
            file_location="/path/to/files",
            filenames=["file1.txt", "file2.txt"],
        )

        experiment_id = "075296f0-9d6a-4bf0-8dbb-80074a255359"
        create_response = Mock()
        create_response.status_code = 201
        create_response.json.return_value = {
            "id": experiment_id,
            "uploads": [
                {
                    "filename": "file1.txt",
                    "url": "http://example.com/upload/file1.txt",
                    "mode": "single",
                },
                {
                    "filename": "file2.txt",
                    "url": "http://example.com/upload/file2.txt",
                    "mode": "single",
                },
            ],
        }

        workflow_response = Mock()
        workflow_response.status_code = 200

        mock_exists.return_value = True
        mock_getsize.side_effect = [1024, 2048]
        mock_upload_response = Mock()
        mock_upload_response.status_code = 200
        mock_requests_put.return_value = mock_upload_response

        mock_client._make_request.side_effect = [create_response, workflow_response]

        result = experiments_resource.create(experiment)

        assert result == experiment_id

        assert mock_client._make_request.call_count == 2

        create_call = mock_client._make_request.call_args_list[0]
        assert create_call[1]["method"] == "POST"
        assert create_call[1]["endpoint"] == "/experiments"
        payload = create_call[1]["json"]["experiment"]
        assert payload["file_location"] == "/path/to/files"
        assert payload["filenames"] == ["file1.txt", "file2.txt"]
        assert payload["file_sizes"] == [None, None]
        assert "s3_bucket" not in payload
        assert "s3_prefix" not in payload

        workflow_call = mock_client._make_request.call_args_list[1]
        assert workflow_call[1]["method"] == "POST"
        assert (
            workflow_call[1]["endpoint"]
            == f"/experiments/{experiment_id}/start_workflow"
        )

        assert mock_requests_put.call_count == 2
        assert mock_exists.call_count == 4
        assert mock_getsize.call_count == 2

    @patch("md_python.uploads.requests.put")
    @patch("md_python.uploads.os.path.getsize")
    @patch("md_python.uploads.os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data=b"file content")
    def test_create_with_multipart_upload(
        self,
        mock_file,
        mock_exists,
        mock_getsize,
        mock_requests_put,
        experiments_resource,
        mock_client,
    ):
        experiment = Experiment(
            name="Multipart Upload Experiment",
            source="test_source",
            file_location="/path/to/files",
            filenames=["large_file.d"],
        )

        experiment_id = "075296f0-9d6a-4bf0-8dbb-80074a255359"
        create_response = Mock()
        create_response.status_code = 201
        create_response.json.return_value = {
            "id": experiment_id,
            "uploads": [
                {
                    "filename": "large_file.d",
                    "mode": "multipart",
                    "upload_session_id": "2~abc123def456ghi789",
                    "parts": [
                        {
                            "url": "https://test-bucket.s3.amazonaws.com/upload/large_file.d?partNumber=1",
                            "part_number": 1,
                        },
                        {
                            "url": "https://test-bucket.s3.amazonaws.com/upload/large_file.d?partNumber=2",
                            "part_number": 2,
                        },
                    ],
                },
            ],
        }

        workflow_response = Mock()
        workflow_response.status_code = 200
        complete_response = Mock()
        complete_response.status_code = 200

        mock_exists.return_value = True
        mock_getsize.return_value = 50_000_000
        mock_upload_response = Mock()
        mock_upload_response.status_code = 200
        mock_upload_response.headers = {"ETag": '"etag123"'}
        mock_requests_put.return_value = mock_upload_response

        mock_client._make_request.side_effect = [
            create_response,
            complete_response,
            workflow_response,
        ]

        result = experiments_resource.create(experiment)

        assert result == experiment_id

        assert mock_client._make_request.call_count == 3

        create_call = mock_client._make_request.call_args_list[0]
        payload = create_call[1]["json"]["experiment"]
        assert payload["file_sizes"] == [50_000_000]

        complete_call = mock_client._make_request.call_args_list[1]
        assert complete_call[1]["method"] == "POST"
        assert (
            complete_call[1]["endpoint"]
            == f"/experiments/{experiment_id}/uploads/complete"
        )
        assert complete_call[1]["json"]["filename"] == "large_file.d"
        assert complete_call[1]["json"]["upload_id"] == "2~abc123def456ghi789"

        workflow_call = mock_client._make_request.call_args_list[2]
        assert (
            workflow_call[1]["endpoint"]
            == f"/experiments/{experiment_id}/start_workflow"
        )

        assert mock_requests_put.call_count == 2
        assert mock_exists.call_count == 2
        assert mock_getsize.call_count == 2

    def test_get_by_id_success(
        self, experiments_resource, sample_experiment_response, mock_client
    ):
        """Test successful experiment retrieval by ID"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_experiment_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_id("1234567890abcdef1234567890abcdef")

        assert isinstance(result, Experiment)
        assert result.name == "Test Experiment"
        assert result.description == "A test experiment for unit testing"
        assert result.source == "test_source"
        assert result.s3_bucket == "test-bucket"
        assert result.s3_prefix == "experiments/test/"
        assert result.filenames == ["file1.txt", "file2.txt"]
        assert result.labelling_method == "manual"
        assert result.status == "active"

        assert result.experiment_design is not None
        assert result.experiment_design.data == [
            ["condition", "replicate"],
            ["control", "1"],
            ["treatment", "1"],
        ]

        assert result.sample_metadata is not None
        assert result.sample_metadata.data == [
            ["sample", "condition"],
            ["sample1", "control"],
            ["sample2", "treatment"],
        ]

        mock_client._make_request.assert_called_once_with(
            method="GET", endpoint="/experiments/1234567890abcdef1234567890abcdef"
        )

    def test_get_by_id_failure(self, experiments_resource, mock_client):
        """Test experiment retrieval failure"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Experiment not found"

        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception) as exc_info:
            experiments_resource.get_by_id("non-existent-id")

        assert "Failed to get experiment: 404 - Experiment not found" in str(
            exc_info.value
        )

    def test_get_by_id_with_missing_optional_fields(
        self, experiments_resource, mock_client
    ):
        """Test experiment retrieval with missing optional fields"""
        minimal_response = {
            "id": "fedcba0987654321fedcba0987654321",
            "name": "Minimal Experiment",
            "source": "minimal_source",
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = minimal_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_id("fedcba0987654321fedcba0987654321")

        assert isinstance(result, Experiment)
        assert result.name == "Minimal Experiment"
        assert result.source == "minimal_source"
        assert result.description is None
        assert result.experiment_design is None
        assert result.sample_metadata is None
        assert result.s3_bucket == ""
        assert result.s3_prefix is None
        assert result.filenames == []
        assert result.labelling_method is None
        assert result.status is None
        assert result.created_at is None

    def test_get_by_id_with_complex_metadata(self, experiments_resource, mock_client):
        """Test experiment retrieval with complex metadata structures"""
        complex_response = {
            "id": "a1b2c3d4e5f67890a1b2c3d4e5f67890",
            "name": "Complex Experiment",
            "source": "complex_source",
            "experiment_design": [
                ["sample_id", "condition", "timepoint", "replicate"],
                ["S001", "control", "0h", "1"],
                ["S002", "control", "0h", "2"],
                ["S003", "treatment", "24h", "1"],
                ["S004", "treatment", "24h", "2"],
            ],
            "sample_metadata": [
                ["sample_id", "patient_id", "age", "gender", "diagnosis"],
                ["S001", "P001", "45", "F", "healthy"],
                ["S002", "P002", "52", "M", "healthy"],
                ["S003", "P003", "38", "F", "disease"],
                ["S004", "P004", "61", "M", "disease"],
            ],
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = complex_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_id("a1b2c3d4e5f67890a1b2c3d4e5f67890")

        assert isinstance(result, Experiment)
        assert result.name == "Complex Experiment"
        assert result.source == "complex_source"

        assert result.experiment_design is not None
        assert len(result.experiment_design.data) == 5
        assert result.experiment_design.data[0] == [
            "sample_id",
            "condition",
            "timepoint",
            "replicate",
        ]
        assert result.experiment_design.data[1] == ["S001", "control", "0h", "1"]

        assert result.sample_metadata is not None
        assert len(result.sample_metadata.data) == 5
        assert result.sample_metadata.data[0] == [
            "sample_id",
            "patient_id",
            "age",
            "gender",
            "diagnosis",
        ]
        assert result.sample_metadata.data[1] == ["S001", "P001", "45", "F", "healthy"]

    def test_get_by_name_success(
        self, experiments_resource, sample_experiment_response, mock_client
    ):
        """Test successful experiment retrieval by name"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_experiment_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_name("Test Experiment")

        assert isinstance(result, Experiment)
        assert result.name == "Test Experiment"
        assert result.description == "A test experiment for unit testing"
        assert result.source == "test_source"
        assert result.s3_bucket == "test-bucket"
        assert result.s3_prefix == "experiments/test/"
        assert result.filenames == ["file1.txt", "file2.txt"]
        assert result.labelling_method == "manual"
        assert result.status == "active"

        assert result.experiment_design is not None
        assert result.experiment_design.data == [
            ["condition", "replicate"],
            ["control", "1"],
            ["treatment", "1"],
        ]

        assert result.sample_metadata is not None
        assert result.sample_metadata.data == [
            ["sample", "condition"],
            ["sample1", "control"],
            ["sample2", "treatment"],
        ]

        mock_client._make_request.assert_called_once_with(
            method="GET", endpoint="/experiments?name=Test Experiment"
        )

    def test_get_by_name_failure(self, experiments_resource, mock_client):
        """Test experiment retrieval by name failure"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Experiment not found"

        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception) as exc_info:
            experiments_resource.get_by_name("Non-existent Experiment")

        assert "Failed to get experiment by name: 404 - Experiment not found" in str(
            exc_info.value
        )

    def test_get_by_name_with_special_characters(
        self, experiments_resource, mock_client
    ):
        """Test experiment retrieval by name with special characters and spaces"""
        special_name_response = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "name": "Test experiment Yansin",
            "description": "Experiment description",
            "labelling_method": "lfq",
            "source": "raw",
            "status": "processing",
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = special_name_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_name("Test experiment Yansin")

        assert isinstance(result, Experiment)
        assert result.name == "Test experiment Yansin"
        assert result.description == "Experiment description"
        assert result.labelling_method == "lfq"
        assert result.source == "raw"
        assert result.status == "processing"

        mock_client._make_request.assert_called_once_with(
            method="GET", endpoint="/experiments?name=Test experiment Yansin"
        )

    def test_get_by_name_with_minimal_response(self, experiments_resource, mock_client):
        """Test experiment retrieval by name with minimal API response"""
        minimal_response = {
            "id": "b2c3d4e5-f6f7-8901-bcde-f23456789012",
            "name": "Minimal Experiment",
            "source": "minimal_source",
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = minimal_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_name("Minimal Experiment")

        assert isinstance(result, Experiment)
        assert result.name == "Minimal Experiment"
        assert result.source == "minimal_source"
        assert result.description is None
        assert result.experiment_design is None
        assert result.sample_metadata is None
        assert result.s3_bucket == ""
        assert result.s3_prefix is None
        assert result.filenames == []
        assert result.labelling_method is None
        assert result.status is None
        assert result.created_at is None

    def test_get_by_name_with_empty_name(self, experiments_resource, mock_client):
        """Test experiment retrieval by name with empty string"""
        empty_name_response = {
            "id": "c3d4e5f6-f7f8-9012-cdef-345678901234",
            "name": "",
            "source": "test_source",
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = empty_name_response

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.get_by_name("")

        assert isinstance(result, Experiment)
        assert result.name == ""
        assert result.source == "test_source"

        mock_client._make_request.assert_called_once_with(
            method="GET", endpoint="/experiments?name="
        )

    def test_update_sample_metadata_success(self, experiments_resource, mock_client):
        """Test successful sample metadata update"""
        sample_metadata = SampleMetadata(
            data=[
                ["sample_name", "dose"],
                ["1", "1"],
                ["2", "20"],
                ["3", "30"],
                ["4", "40"],
                ["5", "50"],
                ["6", "60"],
            ]
        )

        experiment_id = "9022c4b9-f929-4be2-8483-9b2dcb1e76c2"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=sample_metadata
        )

        assert result is True

        mock_client._make_request.assert_called_once()
        call_args = mock_client._make_request.call_args

        assert call_args[1]["method"] == "PUT"
        assert (
            call_args[1]["endpoint"] == f"/experiments/{experiment_id}/sample_metadata"
        )
        assert call_args[1]["headers"] == {
            "Content-Type": "application/json",
            "accept": "application/vnd.md-v1+json",
        }

        payload = call_args[1]["json"]
        assert "sample_metadata" in payload
        assert payload["sample_metadata"] == sample_metadata.data

    def test_update_sample_metadata_failure(self, experiments_resource, mock_client):
        """Test sample metadata update failure"""
        sample_metadata = SampleMetadata(data=[["sample_name", "dose"], ["1", "1"]])

        experiment_id = "invalid-experiment-id"

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Experiment not found"

        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception) as exc_info:
            experiments_resource.update_sample_metadata(
                experiment_id=experiment_id, sample_metadata=sample_metadata
            )

        assert "Failed to update sample metadata: 404 - Experiment not found" in str(
            exc_info.value
        )

    def test_update_sample_metadata_with_empty_metadata(
        self, experiments_resource, mock_client
    ):
        """Test sample metadata update with empty metadata"""
        empty_metadata = SampleMetadata(data=[])

        experiment_id = "9022c4b9-f929-4be2-8483-9b2dcb1e76c2"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=empty_metadata
        )

        assert result is True

        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]
        assert payload["sample_metadata"] == []

    def test_update_sample_metadata_with_complex_metadata(
        self, experiments_resource, mock_client
    ):
        """Test sample metadata update with complex metadata structure"""
        complex_metadata = SampleMetadata(
            data=[
                [
                    "sample_id",
                    "patient_id",
                    "age",
                    "gender",
                    "diagnosis",
                    "treatment_group",
                ],
                ["S001", "P001", "45", "F", "healthy", "control"],
                ["S002", "P002", "52", "M", "healthy", "control"],
                ["S003", "P003", "38", "F", "disease", "treatment"],
                ["S004", "P004", "61", "M", "disease", "treatment"],
            ]
        )

        experiment_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=complex_metadata
        )

        assert result is True

        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]
        assert payload["sample_metadata"] == complex_metadata.data
        assert len(payload["sample_metadata"]) == 5
        assert payload["sample_metadata"][0] == [
            "sample_id",
            "patient_id",
            "age",
            "gender",
            "diagnosis",
            "treatment_group",
        ]

    def test_update_sample_metadata_with_special_characters(
        self, experiments_resource, mock_client
    ):
        """Test sample metadata update with special characters in data"""
        special_metadata = SampleMetadata(
            data=[
                ["sample_name", "description", "notes"],
                ["sample_1", "Control sample", "Normal condition"],
                ["sample_2", "Treatment sample", "High dose (50mg/kg)"],
                ["sample_3", "Special sample", "Contains: NaCl, H2O, pH=7.4"],
            ]
        )

        experiment_id = "b2c3d4e5-f6f7-8901-bcde-f23456789012"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client._make_request.return_value = mock_response

        result = experiments_resource.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=special_metadata
        )

        assert result is True

        call_args = mock_client._make_request.call_args
        payload = call_args[1]["json"]
        assert payload["sample_metadata"] == special_metadata.data
        assert "High dose (50mg/kg)" in payload["sample_metadata"][2]
        assert "Contains: NaCl, H2O, pH=7.4" in payload["sample_metadata"][3]

    def test_update_sample_metadata_headers_verification(
        self, experiments_resource, mock_client
    ):
        """Test that correct headers are sent in the request"""
        sample_metadata = SampleMetadata(data=[["sample_name", "dose"], ["1", "1"]])

        experiment_id = "test-experiment-id"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client._make_request.return_value = mock_response

        experiments_resource.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=sample_metadata
        )

        call_args = mock_client._make_request.call_args
        headers = call_args[1]["headers"]

        assert headers["Content-Type"] == "application/json"
        assert headers["accept"] == "application/vnd.md-v1+json"
        assert len(headers) == 2
