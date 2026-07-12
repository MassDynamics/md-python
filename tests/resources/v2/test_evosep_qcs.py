from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.evosep_qcs import EvosepQcs


class TestV2EvosepQcs:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def evosep_qcs(self, mock_client):
        return EvosepQcs(mock_client)

    def test_create_success(self, evosep_qcs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "id": "11111111-1111-1111-1111-111111111111",
            "filename": "qc.raw",
            "uploaded_by": "user@example.com",
            "created_at": "2026-07-12T00:00:00Z",
        }
        mock_client._make_request.return_value = mock_response

        result = evosep_qcs.create(filename="qc.raw", blob={"metric": 1.23})

        assert result["id"] == "11111111-1111-1111-1111-111111111111"
        assert result["filename"] == "qc.raw"

        call_args = mock_client._make_request.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/evosep_qcs"
        assert call_args[1]["headers"] == {"Content-Type": "application/json"}
        assert call_args[1]["json"] == {
            "filename": "qc.raw",
            "blob": {"metric": 1.23},
        }

    def test_create_feature_flag_off_404(self, evosep_qcs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = '{"error":"Not found"}'
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to create evosep_qc: 404"):
            evosep_qcs.create(filename="qc.raw", blob={})

    def test_create_validation_failure_422(self, evosep_qcs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = '{"errors":["filename is required"]}'
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to create evosep_qc: 422"):
            evosep_qcs.create(filename="", blob={})

    def test_create_server_error(self, evosep_qcs, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal error"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to create evosep_qc: 500"):
            evosep_qcs.create(filename="qc.raw", blob={"metric": 1.0})
