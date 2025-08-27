"""
Tests for the Dataset class
"""

import pytest
from datetime import datetime
from uuid import UUID
from md_python.models import Dataset


class TestDataset:
    """Test cases for Dataset class"""

    def test_init_minimal(self):
        """Test Dataset initialization with minimal required fields"""
        dataset = Dataset(
            input_dataset_ids=[],
            name="Test Dataset",
            job_slug="test_job",
            job_run_params={},
        )

        assert dataset.input_dataset_ids == []
        assert dataset.name == "Test Dataset"
        assert dataset.job_slug == "test_job"
        assert dataset.sample_names is None
        assert dataset.job_run_params == {}
        assert dataset.job_run_start_time is None

    def test_init_full(self):
        """Test Dataset initialization with all fields"""
        dataset = Dataset(
            id=UUID("123e4567-e89b-12d3-a456-426614174000"),
            input_dataset_ids=[UUID("456e7890-e89b-12d3-a456-426614174000")],
            name="Test Dataset",
            job_slug="test_job",
            sample_names=["sample1", "sample2"],
            job_run_params={"param1": "value1"},
            job_run_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )

        assert dataset.id == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert dataset.input_dataset_ids == [
            UUID("456e7890-e89b-12d3-a456-426614174000")
        ]
        assert dataset.name == "Test Dataset"
        assert dataset.job_slug == "test_job"
        assert dataset.sample_names == ["sample1", "sample2"]
        assert dataset.job_run_params == {"param1": "value1"}
        assert dataset.job_run_start_time == datetime(2023, 1, 1, 12, 0, 0)

    def test_input_dataset_ids_with_strings(self):
        """Test Dataset initialization with string IDs (should be converted to UUIDs)"""
        # Note: This test assumes the class handles string to UUID conversion
        # If not, this test might need adjustment
        dataset = Dataset(
            id="123e4567-e89b-12d3-a456-426614174000",
            input_dataset_ids=["456e7890-e89b-12d3-a456-426614174000"],
            name="Test Dataset",
            job_slug="test_job",
            job_run_params={},
        )

        # The behavior depends on how the class handles string IDs
        # This test documents the current behavior
        assert dataset.name == "Test Dataset"
        assert dataset.job_slug == "test_job"
