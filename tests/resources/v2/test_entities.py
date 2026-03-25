from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.entities import Entities


class TestEntities:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def entities(self, mock_client):
        return Entities(mock_client)

    def _mock_response(self, mock_client, status_code, body):
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.json.return_value = body
        mock_response.text = str(body)
        mock_client._make_request.return_value = mock_response
        return mock_response

    def test_search_returns_results(self, entities, mock_client):
        results = [
            {
                "dataset_id": "abc123",
                "entity_type": "protein",
                "items": [
                    {
                        "ProteinIds": ["P12345"],
                        "GeneNames": ["BRCA1"],
                        "Description": "Breast cancer type 1",
                        "GroupId": "1",
                    }
                ],
            }
        ]
        self._mock_response(mock_client, 200, {"results": results})

        output = entities.search("BRCA1", ["abc123"])

        assert output == results
        call_args = mock_client._make_request.call_args[1]
        assert call_args["method"] == "POST"
        assert call_args["endpoint"] == "/entities/search"
        assert call_args["json"] == {"keyword": "BRCA1", "dataset_ids": ["abc123"]}

    def test_search_returns_empty_when_no_results(self, entities, mock_client):
        self._mock_response(mock_client, 200, {"results": []})
        output = entities.search("UNKNOWN_XYZ", ["abc123"])
        assert output == []

    def test_search_raises_value_error_on_400(self, entities, mock_client):
        self._mock_response(mock_client, 400, {"error": "keyword too short"})
        with pytest.raises(ValueError, match="400"):
            entities.search("X", ["abc123"])

    def test_search_raises_value_error_on_404(self, entities, mock_client):
        self._mock_response(mock_client, 404, {"error": "Not found"})
        with pytest.raises(ValueError, match="404"):
            entities.search("BRCA1", ["nonexistent-id"])

    def test_search_raises_permission_error_on_403(self, entities, mock_client):
        self._mock_response(mock_client, 403, {"error": "Forbidden"})
        with pytest.raises(PermissionError, match="not enabled"):
            entities.search("BRCA1", ["abc123"])

    def test_search_raises_exception_on_502(self, entities, mock_client):
        self._mock_response(mock_client, 502, {"error": "Bad gateway"})
        with pytest.raises(Exception, match="502"):
            entities.search("BRCA1", ["abc123"])
