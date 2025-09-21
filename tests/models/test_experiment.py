"""
Tests for the Experiment class
"""

from datetime import datetime
from uuid import UUID

import pytest

from md_python.models import Experiment, ExperimentDesign, SampleMetadata


def test_experiment_design_filenames_extraction():
    design = ExperimentDesign(
        data=[
            ["filename", "sample_name", "condition"],
            ["MD00001_0", "A", "cond1"],
            ["MD00002_0", "B", "cond2"],
        ]
    )

    assert design.filenames() == ["MD00001_0", "MD00002_0"]


class TestExperiment:
    """Test cases for Experiment class"""

    def test_init_minimal(self):
        """Test Experiment initialization with minimal required fields"""
        experiment = Experiment(
            name="Test Experiment",
            source="test_source",
            s3_bucket="test-bucket",
            filenames=[],
        )

        assert experiment.name == "Test Experiment"
        assert experiment.source == "test_source"
        assert experiment.s3_bucket == "test-bucket"
        assert experiment.filenames == []
        assert experiment.id is None
        assert experiment.description is None
        assert experiment.experiment_design is None
        assert experiment.labelling_method is None
        assert experiment.s3_prefix is None
        assert experiment.sample_metadata is None
        assert experiment.created_at is None
        assert experiment.status is None

    def test_init_full(self):
        """Test Experiment initialization with all fields"""
        experiment_design = ExperimentDesign(data=[["sample1", "condition1"]])
        sample_metadata = SampleMetadata(data=[["sample2", "condition2"]])
        experiment = Experiment(
            name="Test Experiment",
            source="test_source",
            id=UUID("123e4567-e89b-12d3-a456-426614174000"),
            description="A test experiment",
            experiment_design=experiment_design,
            labelling_method="manual",
            s3_bucket="test-bucket",
            s3_prefix="test/prefix",
            filenames=["file1.txt", "file2.txt"],
            sample_metadata=sample_metadata,
            created_at=datetime(2023, 1, 1, 12, 0, 0),
            status="active",
        )

        assert experiment.name == "Test Experiment"
        assert experiment.source == "test_source"
        assert experiment.id == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert experiment.description == "A test experiment"
        assert experiment.experiment_design == experiment_design
        assert experiment.labelling_method == "manual"
        assert experiment.s3_bucket == "test-bucket"
        assert experiment.s3_prefix == "test/prefix"
        assert experiment.filenames == ["file1.txt", "file2.txt"]
        assert experiment.sample_metadata == sample_metadata
        assert experiment.created_at == datetime(2023, 1, 1, 12, 0, 0)
        assert experiment.status == "active"

    def test_str_minimal(self):
        """Test string representation with minimal fields"""
        experiment = Experiment(
            name="Test Experiment",
            source="test_source",
            s3_bucket="test-bucket",
            filenames=[],
        )
        result = str(experiment)

        expected_lines = [
            "Experiment: Test Experiment",
            "Source: test_source",
            "S3 Bucket: test-bucket",
        ]

        for line in expected_lines:
            assert line in result

    def test_str_full(self):
        """Test string representation with all fields"""
        experiment_design = ExperimentDesign(data=[["sample1", "condition1"]])
        sample_metadata = SampleMetadata(data=[["sample2", "condition2"]])
        experiment = Experiment(
            name="Test Experiment",
            source="test_source",
            id=UUID("123e4567-e89b-12d3-a456-426614174000"),
            description="A test experiment",
            experiment_design=experiment_design,
            labelling_method="manual",
            s3_bucket="test-bucket",
            s3_prefix="test/prefix",
            filenames=["file1.txt", "file2.txt"],
            sample_metadata=sample_metadata,
            created_at=datetime(2023, 1, 1, 12, 0, 0),
            status="active",
        )
        result = str(experiment)

        expected_lines = [
            "Experiment: Test Experiment",
            "ID: 123e4567-e89b-12d3-a456-426614174000",
            "Description: A test experiment",
            "Source: test_source",
            "Status: active",
            "Labelling Method: manual",
            "Created: 2023-01-01 12:00:00",
            "S3 Bucket: test-bucket",
            "S3 Prefix: test/prefix",
            "Files: 2 files",
            "Experiment Design:",
            "Sample Metadata:",
        ]

        for line in expected_lines:
            assert line in result

    def test_from_json_success(self):
        """Test creating Experiment from JSON data"""
        json_data = {
            "id": "123e4567-e89b-12d3-a456-426614174000",
            "name": "Test Experiment",
            "description": "A test experiment",
            "labelling_method": "manual",
            "source": "test_source",
            "sample_metadata": [["sample1", "condition1"]],
            "created_at": "2023-01-01T12:00:00Z",
            "status": "active",
        }

        experiment = Experiment.from_json(json_data)

        assert experiment.id == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert experiment.name == "Test Experiment"
        assert experiment.description == "A test experiment"
        assert experiment.labelling_method == "manual"
        assert experiment.source == "test_source"
        assert experiment.status == "active"
        assert experiment.sample_metadata is not None
        assert experiment.sample_metadata.data == [["sample1", "condition1"]]
        assert experiment.created_at is not None
        # Note: The exact datetime comparison might need adjustment based on timezone handling

    def test_from_json_minimal(self):
        """Test creating Experiment from minimal JSON data"""
        json_data = {
            "id": "123e4567-e89b-12d3-a456-426614174000",
            "name": "Test Experiment",
            "source": "test_source",
        }

        experiment = Experiment.from_json(json_data)

        assert experiment.id == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert experiment.name == "Test Experiment"
        assert experiment.source == "test_source"
        assert experiment.description is None
        assert experiment.labelling_method is None
        assert experiment.status is None
        assert experiment.sample_metadata is None
        assert experiment.created_at is None

    def test_from_json_missing_optional_fields(self):
        """Test creating Experiment from JSON with missing optional fields"""
        json_data = {
            "id": "123e4567-e89b-12d3-a456-426614174000",
            "name": "Test Experiment",
            "source": "test_source",
            "created_at": "2023-01-01T12:00:00Z",
        }

        experiment = Experiment.from_json(json_data)

        assert experiment.id == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert experiment.name == "Test Experiment"
        assert experiment.source == "test_source"
        assert experiment.description is None
        assert experiment.labelling_method is None
        assert experiment.status is None
        assert experiment.sample_metadata is None
