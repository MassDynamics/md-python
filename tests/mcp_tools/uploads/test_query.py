"""Tests for query_uploads."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.uploads import query_uploads


def _mock_response(data=None, pagination=None):
    return {
        "data": data or [],
        "pagination": pagination
        or {
            "current_page": 1,
            "per_page": 50,
            "total_count": 0,
            "total_pages": 0,
        },
    }


class TestQueryUploads:
    def test_filters_forwarded_to_client(self):
        mock_client = MagicMock()
        mock_client.uploads.query.return_value = _mock_response()
        with patch("mcp_tools.uploads.query.get_client", return_value=mock_client):
            query_uploads(
                status=["completed"],
                source=["diann_tabular"],
                search="brca",
                sample_metadata=[{"column": "condition", "value": "treated"}],
                page=3,
            )
        mock_client.uploads.query.assert_called_once_with(
            status=["completed"],
            source=["diann_tabular"],
            search="brca",
            sample_metadata=[{"column": "condition", "value": "treated"}],
            page=3,
        )

    def test_pagination_metadata_surfaces(self):
        mock_client = MagicMock()
        mock_client.uploads.query.return_value = _mock_response(
            data=[
                {
                    "id": "u1",
                    "name": "a",
                    "status": "completed",
                    "source": "x",
                    "created_at": "2026-01-01",
                    "extra_field": "ignored",
                }
            ],
            pagination={
                "current_page": 2,
                "per_page": 50,
                "total_count": 75,
                "total_pages": 2,
            },
        )
        with patch("mcp_tools.uploads.query.get_client", return_value=mock_client):
            result = json.loads(query_uploads(page=2))

        assert result["page"] == 2
        assert result["total_pages"] == 2
        assert result["total_count"] == 75

    def test_records_are_projected(self):
        mock_client = MagicMock()
        mock_client.uploads.query.return_value = _mock_response(
            data=[
                {
                    "id": "u1",
                    "name": "experiment one",
                    "status": "completed",
                    "source": "diann_tabular",
                    "created_at": "2026-01-01T00:00:00Z",
                    "sample_metadata": "should be dropped",
                    "description": "should be dropped",
                }
            ]
        )
        with patch("mcp_tools.uploads.query.get_client", return_value=mock_client):
            result = json.loads(query_uploads())

        assert result["uploads"] == [
            {
                "id": "u1",
                "name": "experiment one",
                "status": "completed",
                "source": "diann_tabular",
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]

    def test_empty_data(self):
        mock_client = MagicMock()
        mock_client.uploads.query.return_value = _mock_response()
        with patch("mcp_tools.uploads.query.get_client", return_value=mock_client):
            result = json.loads(query_uploads())
        assert result["uploads"] == []

    def test_error_returns_error_dict(self):
        mock_client = MagicMock()
        mock_client.uploads.query.side_effect = Exception("boom")
        with patch("mcp_tools.uploads.query.get_client", return_value=mock_client):
            result = json.loads(query_uploads())
        assert "error" in result
        assert "boom" in result["error"]
