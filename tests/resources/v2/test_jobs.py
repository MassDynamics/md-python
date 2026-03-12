from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.jobs import Jobs


class TestV2Jobs:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def jobs(self, mock_client):
        return Jobs(mock_client)

    def test_list_success(self, jobs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": 1, "name": "Demo Flow", "slug": "demo_flow"},
            {"id": 2, "name": "Pairwise Comparison", "slug": "pairwise_comparison"},
        ]
        mock_client._make_request.return_value = mock_response

        result = jobs.list()

        assert len(result) == 2
        assert result[0]["slug"] == "demo_flow"
        assert result[1]["slug"] == "pairwise_comparison"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == "/jobs"

    def test_list_empty(self, jobs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_client._make_request.return_value = mock_response

        result = jobs.list()

        assert result == []

    def test_list_failure(self, jobs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to list jobs: 500"):
            jobs.list()
