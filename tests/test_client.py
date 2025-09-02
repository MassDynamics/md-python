from unittest.mock import Mock, patch

import pytest
import requests

from md_python.client import MDClient


class TestMDClient:
    """Test cases for MDClient class"""

    def test_init(self):
        """Test client initialization"""
        api_token = "test_token_123"
        client = MDClient(api_token)

        assert client.api_token == api_token
        assert client.base_url == "https://app.massdynamics.com/api"
        assert hasattr(client, "health")
        assert hasattr(client, "experiments")
        assert hasattr(client, "datasets")

    def test_get_headers(self):
        """Test header generation"""
        api_token = "test_token_123"
        client = MDClient(api_token)

        headers = client._get_headers()

        assert headers["accept"] == "application/vnd.md-v1+json"
        assert headers["Authorization"] == f"Bearer {api_token}"

    @patch("requests.request")
    def test_make_request_basic(self, mock_request):
        """Test basic request functionality"""
        api_token = "test_token_123"
        client = MDClient(api_token)

        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        # Make request
        response = client._make_request("GET", "/test-endpoint")

        # Verify request was made correctly
        mock_request.assert_called_once_with(
            "GET",
            "https://app.massdynamics.com/api/test-endpoint",
            headers=client._get_headers(),
            json=None,
        )
        assert response == mock_response

    @patch("requests.request")
    def test_make_request_with_custom_headers(self, mock_request):
        """Test request with custom headers"""
        api_token = "test_token_123"
        client = MDClient(api_token)

        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        # Custom headers
        custom_headers = {"Content-Type": "application/json"}

        # Make request
        response = client._make_request(
            "POST", "/test-endpoint", headers=custom_headers
        )

        # Verify headers were merged correctly
        expected_headers = client._get_headers()
        expected_headers.update(custom_headers)

        mock_request.assert_called_once_with(
            "POST",
            "https://app.massdynamics.com/api/test-endpoint",
            headers=expected_headers,
            json=None,
        )
        assert response == mock_response

    @patch("requests.request")
    def test_make_request_with_json(self, mock_request):
        """Test request with json data"""
        api_token = "test_token_123"
        client = MDClient(api_token)

        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        # JSON data
        json_data = {"key": "value", "number": 42}

        # Make request
        response = client._make_request("POST", "/test-endpoint", json=json_data)

        # Verify json was passed through
        mock_request.assert_called_once_with(
            "POST",
            "https://app.massdynamics.com/api/test-endpoint",
            headers=client._get_headers(),
            json=json_data,
        )
        assert response == mock_response

    def test_base_url_formatting(self):
        """Test that base URL is properly formatted"""
        client = MDClient("test_token")

        # Test endpoint concatenation
        endpoint = "/health"
        expected_url = "https://app.massdynamics.com/api/health"

        with patch("requests.request") as mock_request:
            mock_response = Mock()
            mock_request.return_value = mock_response

            client._make_request("GET", endpoint)

            mock_request.assert_called_once_with(
                "GET", expected_url, headers=client._get_headers(), json=None
            )

    def test_api_token_in_authorization_header(self):
        """Test that API token is properly included in Authorization header"""
        api_token = "secret_token_456"
        client = MDClient(api_token)

        headers = client._get_headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == f"Bearer {api_token}"
        assert api_token in headers["Authorization"]

    def test_custom_base_url(self):
        """Test that client can be initialized with a custom base URL"""
        api_token = "test_token_123"
        custom_base_url = "https://custom.example.com/api"

        client = MDClient(api_token, base_url=custom_base_url)

        assert client.api_token == api_token
        assert client.base_url == custom_base_url
        assert hasattr(client, "health")
        assert hasattr(client, "experiments")
        assert hasattr(client, "datasets")

    @patch("requests.request")
    def test_custom_base_url_request(self, mock_request):
        """Test that requests use the custom base URL when provided"""
        api_token = "test_token_123"
        custom_base_url = "https://custom.example.com/api"
        client = MDClient(api_token, base_url=custom_base_url)

        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        # Make request
        response = client._make_request("GET", "/test-endpoint")

        # Verify request was made with custom base URL
        mock_request.assert_called_once_with(
            "GET",
            f"{custom_base_url}/test-endpoint",
            headers=client._get_headers(),
            json=None,
        )
        assert response == mock_response
