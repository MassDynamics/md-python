"""Tests for get_dataset."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from mcp_tools.datasets import get_dataset

from .conftest import mock_dataset


class TestGetDataset:
    def test_returns_full_dataset_record_with_job_run_params(self):
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.return_value = mock_dataset(
            id="ds-42",
            name="CKD demo imputation",
            type="INTENSITY",
            state="COMPLETED",
            job_slug="",
            job_run_params={
                "entity_type": "protein",
                "normalisation_methods_proteomics": "skip",
                "imputation_methods": "mnar",
                "std_position": 1.8,
                "std_width": 0.3,
            },
            input_dataset_ids=["ds-input"],
            sample_names=["s1", "s2"],
        )
        with patch("mcp_tools.datasets.get.get_client", return_value=mock_client):
            raw = get_dataset("ds-42")
        mock_client.datasets.get_by_id.assert_called_once_with("ds-42")
        payload = json.loads(raw)
        assert payload["id"] == "ds-42"
        assert payload["name"] == "CKD demo imputation"
        assert payload["type"] == "INTENSITY"
        assert payload["state"] == "COMPLETED"
        assert payload["job_slug"] == ""
        assert payload["job_run_params"]["normalisation_methods_proteomics"] == "skip"
        assert payload["job_run_params"]["imputation_methods"] == "mnar"
        assert payload["input_dataset_ids"] == ["ds-input"]
        assert payload["sample_names"] == ["s1", "s2"]

    def test_serialises_job_run_start_time_as_iso_string(self):
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.return_value = mock_dataset(
            job_run_start_time=datetime(2026, 5, 19, 4, 10, 0, tzinfo=timezone.utc),
        )
        with patch("mcp_tools.datasets.get.get_client", return_value=mock_client):
            payload = json.loads(get_dataset("ds-1"))
        assert payload["job_run_start_time"] == "2026-05-19T04:10:00+00:00"

    def test_returns_error_envelope_on_404(self):
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.return_value = None
        with patch("mcp_tools.datasets.get.get_client", return_value=mock_client):
            payload = json.loads(get_dataset("missing-id"))
        assert payload == {"error": "Dataset not found", "dataset_id": "missing-id"}

    def test_returns_error_envelope_on_http_failure(self):
        mock_client = MagicMock()
        mock_client.datasets.get_by_id.side_effect = Exception(
            "Failed to get dataset: 500 - boom"
        )
        with patch("mcp_tools.datasets.get.get_client", return_value=mock_client):
            payload = json.loads(get_dataset("ds-1"))
        assert payload["dataset_id"] == "ds-1"
        assert "Failed to get dataset" in payload["error"]
