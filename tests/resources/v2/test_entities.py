from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.entities import Entities


class TestV2Entities:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def entities(self, mock_client):
        return Entities(mock_client)

    def test_query_success(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"gene_name": "BRCA1", "dataset_id": "abc-123"},
                {"gene_name": "BRCA1", "dataset_id": "def-456"},
            ]
        }
        mock_client._make_request.return_value = mock_response

        result = entities.query(keyword="BRCA1", dataset_ids=["abc-123", "def-456"])

        assert "results" in result
        assert len(result["results"]) == 2
        assert result["results"][0]["gene_name"] == "BRCA1"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/entities/query"
        assert call_args[1]["json"] == {
            "keyword": "BRCA1",
            "dataset_ids": ["abc-123", "def-456"],
        }

    def test_query_empty_results(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_client._make_request.return_value = mock_response

        result = entities.query(keyword="NONEXISTENT", dataset_ids=["abc-123"])

        assert result == {"results": []}

    def test_query_single_dataset(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"gene_name": "TP53"}]}
        mock_client._make_request.return_value = mock_response

        result = entities.query(keyword="TP53", dataset_ids=["abc-123"])

        call_args = mock_client._make_request.call_args
        assert call_args[1]["json"]["dataset_ids"] == ["abc-123"]
        assert len(result["results"]) == 1

    def test_query_failure(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "Entity search failed"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to query entities: 502"):
            entities.query(keyword="BRCA1", dataset_ids=["abc-123"])

    def test_query_forbidden(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to query entities: 403"):
            entities.query(keyword="BRCA1", dataset_ids=["abc-123"])

    def test_query_bad_request(self, entities, mock_client):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "keyword is too short"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to query entities: 400"):
            entities.query(keyword="A", dataset_ids=["abc-123"])
