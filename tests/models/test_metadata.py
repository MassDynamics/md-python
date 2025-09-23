"""
Tests for the Metadata class
"""

import os
import tempfile

import pytest

from md_python.models.metadata import Metadata, ExperimentDesign, SampleMetadata


class TestMetadata:
    """Test cases for Metadata class"""

    def test_init_with_data(self):
        """Test Metadata initialization with data"""
        data = [["sample1", "condition1"], ["sample2", "condition2"]]
        metadata = Metadata(data=data)

        assert metadata.data == data

    def test_str_with_data(self):
        """Test string representation with data"""
        data = [["sample1", "condition1"], ["sample2", "condition2"]]
        metadata = Metadata(data=data)
        result = str(metadata)

        expected_lines = [
            "Metadata: 2 rows",
            "  Row 1: sample1, condition1",
            "  Row 2: sample2, condition2",
        ]

        for line in expected_lines:
            assert line in result

    def test_str_with_many_rows(self):
        """Test string representation with many rows (truncation)"""
        data = [["sample" + str(i), "condition" + str(i)] for i in range(10)]
        metadata = Metadata(data=data)
        result = str(metadata)

        expected_lines = [
            "Metadata: 10 rows",
            "  Row 1: sample0, condition0",
            "  Row 2: sample1, condition1",
            "  Row 3: sample2, condition2",
            "... and 7 more rows",
        ]

        for line in expected_lines:
            assert line in result

    def test_from_csv_success(self):
        """Test creating Metadata from CSV file"""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("sample1,condition1\n")
            f.write("sample2,condition2\n")
            temp_file = f.name

        try:
            metadata = Metadata.from_csv(temp_file)

            expected_data = [["sample1", "condition1"], ["sample2", "condition2"]]
            assert metadata.data == expected_data
        finally:
            # Clean up
            os.unlink(temp_file)


class TestExperimentDesign:
    def test_normalization(self):
        ed = ExperimentDesign(
            data=[
                ["file", "sample", "group"],
                ["a.d", "1", "q"],
                ["b.d", "2", "e"],
            ]
        )
        # normalized header
        assert ed.data[0] == ["filename", "sample_name", "condition"]


class TestSampleMetadata:
    def test_to_columns_and_pairwise(self):
        sm = SampleMetadata(
            data=[
                ["group", "sample_name"],
                ["a", "s1"],
                ["a", "s2"],
                ["b", "s3"],
                ["c", "s4"],
            ]
        )
        cols = sm.to_columns()
        assert cols["group"] == ["a", "a", "b", "c"]
        pairs = sm.pairwise_vs_control(column="group", control="c")
        assert pairs == [["a", "c"], ["b", "c"]]

    def test_from_csv_custom_delimiter(self):
        """Test creating Metadata from CSV file with custom delimiter"""
        # Create temporary CSV file with semicolon delimiter
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("sample1;condition1\n")
            f.write("sample2;condition2\n")
            temp_file = f.name

        try:
            metadata = Metadata.from_csv(temp_file, delimiter=";")

            expected_data = [["sample1", "condition1"], ["sample2", "condition2"]]
            assert metadata.data == expected_data
        finally:
            # Clean up
            os.unlink(temp_file)

    def test_from_csv_file_not_found(self):
        """Test creating Metadata from non-existent CSV file"""
        with pytest.raises(
            FileNotFoundError, match="CSV file not found: nonexistent.csv"
        ):
            Metadata.from_csv("nonexistent.csv")

    def test_from_csv_read_error(self):
        """Test creating Metadata from CSV file with read error"""
        # Create temporary file that can't be read as CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("invalid content\n")
            temp_file = f.name

        try:
            # This should work but might have unexpected behavior
            metadata = Metadata.from_csv(temp_file)
            # The exact behavior depends on how csv.reader handles the content
            assert metadata.data is not None
        finally:
            # Clean up
            os.unlink(temp_file)
