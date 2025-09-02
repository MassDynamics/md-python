from unittest.mock import Mock

import pytest

from md_python.client import MDClient
from md_python.resources.health import Health


class TestHealth:
    """Test cases for Health resource"""

    @pytest.fixture
    def mock_client(self):
        """Create a mock MDClient for testing"""
        client = Mock(spec=MDClient)
        return client

    @pytest.fixture
    def health_resource(self, mock_client):
        """Create Health resource instance with mock client"""
        return Health(mock_client)

    @pytest.fixture
    def sample_health_response(self):
        """Sample API response for health check"""
        return {
            "status": "healthy",
            "timestamp": "2024-01-01T00:00:00Z",
            "version": "1.0.0",
            "uptime": 3600,
        }

    def test_check_success(self, health_resource, sample_health_response, mock_client):
        """Test successful health check"""
        # Mock the API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_health_response

        mock_client._make_request.return_value = mock_response

        # Call the check method
        result = health_resource.check()

        # Verify the result
        assert result == sample_health_response

        # Verify the API call was made correctly
        mock_client._make_request.assert_called_once_with("GET", "/health")

    def test_check_with_different_status_codes(self, health_resource, mock_client):
        """Test health check with different successful status codes"""
        # Test with 200 status
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"status": "healthy"}

        mock_client._make_request.return_value = mock_response_200

        result = health_resource.check()
        assert result == {"status": "healthy"}

        # Test with 204 status (no content)
        mock_response_204 = Mock()
        mock_response_204.status_code = 204
        mock_response_204.json.return_value = {}

        mock_client._make_request.return_value = mock_response_204

        result = health_resource.check()
        assert result == {}

    def test_check_with_http_error(self, health_resource, mock_client):
        """Test health check with HTTP error response"""
        # Mock the API response with HTTP error
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Internal Server Error")

        mock_client._make_request.return_value = mock_response

        # Call the check method
        result = health_resource.check()

        # Verify the result contains error information
        assert result["status"] == "error"
        assert "Internal Server Error" in result["message"]

        # Verify the API call was made
        mock_client._make_request.assert_called_once_with("GET", "/health")

    def test_check_with_network_error(self, health_resource, mock_client):
        """Test health check with network/connection error"""
        # Mock the API call to raise a network error
        mock_client._make_request.side_effect = Exception("Connection refused")

        # Call the check method
        result = health_resource.check()

        # Verify the result contains error information
        assert result["status"] == "error"
        assert "Connection refused" in result["message"]

        # Verify the API call was attempted
        mock_client._make_request.assert_called_once_with("GET", "/health")

    def test_check_with_timeout_error(self, health_resource, mock_client):
        """Test health check with timeout error"""
        # Mock the API call to raise a timeout error
        mock_client._make_request.side_effect = Exception("Request timeout")

        # Call the check method
        result = health_resource.check()

        # Verify the result contains error information
        assert result["status"] == "error"
        assert "Request timeout" in result["message"]

    def test_check_with_json_decode_error(self, health_resource, mock_client):
        """Test health check with JSON decode error"""
        # Mock the API response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = Exception("Invalid JSON")

        mock_client._make_request.return_value = mock_response

        # Call the check method
        result = health_resource.check()

        # Verify the result contains error information
        assert result["status"] == "error"
        assert "Invalid JSON" in result["message"]

    def test_check_with_empty_response(self, health_resource, mock_client):
        """Test health check with empty response"""
        # Mock the API response with empty data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        mock_client._make_request.return_value = mock_response

        # Call the check method
        result = health_resource.check()

        # Verify the result
        assert result == {}

    def test_check_with_complex_health_data(self, health_resource, mock_client):
        """Test health check with complex health status data"""
        # Mock the API response with complex health data
        complex_health_response = {
            "status": "degraded",
            "timestamp": "2024-01-01T00:00:00Z",
            "version": "2.1.0",
            "uptime": 86400,
            "services": {
                "database": "healthy",
                "cache": "degraded",
                "storage": "healthy",
            },
            "metrics": {"cpu_usage": 45.2, "memory_usage": 78.9, "disk_usage": 23.1},
            "warnings": [
                "Cache service experiencing high latency",
                "Memory usage above 75%",
            ],
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = complex_health_response

        mock_client._make_request.return_value = mock_response

        # Call the check method
        result = health_resource.check()

        # Verify the result
        assert result == complex_health_response
        assert result["status"] == "degraded"
        assert "services" in result
        assert "metrics" in result
        assert "warnings" in result
        assert len(result["warnings"]) == 2

    def test_health_resource_initialization(self, mock_client):
        """Test Health resource initialization"""
        health = Health(mock_client)
        assert health._client == mock_client

    def test_check_method_signature(self, health_resource):
        """Test that check method has correct signature and return type"""
        import inspect

        # Check method signature
        sig = inspect.signature(health_resource.check)
        # Check that return type contains Dict[str, Any] in any format
        return_annotation_str = str(sig.return_annotation)
        assert (
            "Dict" in return_annotation_str
            and "str" in return_annotation_str
            and "Any" in return_annotation_str
        )

        # Check that method takes no parameters (the self parameter is not counted in inspect.signature)
        assert len(sig.parameters) == 0
