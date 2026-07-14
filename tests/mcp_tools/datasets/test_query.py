"""Tests for query_datasets."""

import json
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import query_datasets


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


class TestQueryDatasets:
    def test_filters_forwarded_to_client(self):
        mock_client = MagicMock()
        mock_client.datasets.query.return_value = _mock_response()
        with patch("mcp_tools.datasets.query.get_client", return_value=mock_client):
            query_datasets(
                upload_id="u-1",
                state=["COMPLETED"],
                type=["PAIRWISE"],
                search="treated vs control",
                page=2,
            )
        mock_client.datasets.query.assert_called_once_with(
            upload_id="u-1",
            state=["COMPLETED"],
            type=["PAIRWISE"],
            search="treated vs control",
            page=2,
        )

    def test_pagination_metadata_surfaces(self):
        mock_client = MagicMock()
        mock_client.datasets.query.return_value = _mock_response(
            pagination={
                "current_page": 1,
                "per_page": 50,
                "total_count": 9,
                "total_pages": 1,
            }
        )
        with patch("mcp_tools.datasets.query.get_client", return_value=mock_client):
            result = json.loads(query_datasets())
        assert result["page"] == 1
        assert result["total_pages"] == 1
        assert result["total_count"] == 9

    def test_records_are_projected(self):
        mock_client = MagicMock()
        mock_client.datasets.query.return_value = _mock_response(
            data=[
                {
                    "id": "ds-1",
                    "name": "NI dataset",
                    "type": "NORMALISATION_AND_IMPUTATION",
                    "state": "COMPLETED",
                    "experiment_id": "u-1",
                    "created_at": "2026-01-01T00:00:00Z",
                    "job_run_params": "should be dropped",
                    "input_dataset_ids": ["ds-0"],
                }
            ]
        )
        with patch("mcp_tools.datasets.query.get_client", return_value=mock_client):
            result = json.loads(query_datasets())

        assert result["datasets"] == [
            {
                "id": "ds-1",
                "name": "NI dataset",
                "type": "NORMALISATION_AND_IMPUTATION",
                "state": "COMPLETED",
                "experiment_id": "u-1",
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]

    def test_empty_data(self):
        mock_client = MagicMock()
        mock_client.datasets.query.return_value = _mock_response()
        with patch("mcp_tools.datasets.query.get_client", return_value=mock_client):
            result = json.loads(query_datasets())
        assert result["datasets"] == []

    def test_error_returns_error_dict(self):
        mock_client = MagicMock()
        mock_client.datasets.query.side_effect = Exception("boom")
        with patch("mcp_tools.datasets.query.get_client", return_value=mock_client):
            result = json.loads(query_datasets())
        assert "error" in result
        assert "boom" in result["error"]


class TestTypeFilterEnumDocstring:
    """The server enum REJECTS ANOVA — advertising it produced a 400 every time.

    Rails source of truth: workflow/app/api/api/v2/datasets/query.rb:43.
    """

    ACCEPTED = [
        "DEMO",
        "DOSE_RESPONSE",
        "DOSE_RESPONSE_AGGREGATE",
        "ENRICHMENT",
        "IMPUTATION",
        "INTENSITY",
        "NORMALISATION_AND_IMPUTATION",
        "PAIRWISE",
    ]

    def test_docstring_lists_every_accepted_value(self):
        doc = query_datasets.__doc__ or ""
        type_doc = doc.split("type:", 1)[1].split("search:", 1)[0]
        for value in self.ACCEPTED:
            assert value in type_doc

    def test_docstring_says_anova_is_not_filterable(self):
        doc = query_datasets.__doc__ or ""
        type_doc = doc.split("type:", 1)[1].split("search:", 1)[0]
        assert "400" in type_doc
        assert 'type=["ANOVA"]' in type_doc
        # ...but a dataset's own type CAN be ANOVA — say so, or the next reader
        # deletes ANOVA from the table catalogue.
        assert "CAN be ANOVA" in type_doc
