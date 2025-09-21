"""
Metadata model for handling 2D array data with CSV import capabilities
"""

import csv
from abc import ABC
from dataclasses import dataclass
from typing import List

from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass
@dataclass
class Metadata(ABC):
    """Metadata class that handles 2D array data with CSV import capabilities"""

    data: List[List[str]]

    def __str__(self) -> str:
        """Return a readable string representation of the metadata"""

        lines = [f"Metadata: {len(self.data)} rows"]
        if self.data:
            # Show first few rows as preview
            preview_rows = min(3, len(self.data))
            for i in range(preview_rows):
                lines.append(f"  Row {i+1}: {', '.join(self.data[i])}")

            if len(self.data) > 3:
                lines.append(f"  ... and {len(self.data) - 3} more rows")

        return "\n".join(lines)

    @classmethod
    def from_csv(cls, file_path: str, delimiter: str = ",") -> "Metadata":
        """
        Create Metadata object from CSV file

        Args:
            file_path: Path to the CSV file
            delimiter: CSV delimiter (default: ',')

        Returns:
            Metadata object with data loaded from CSV
        """
        data = []
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                reader = csv.reader(file, delimiter=delimiter)
                data = [row for row in reader]
        except FileNotFoundError as e:
            raise FileNotFoundError(f"CSV file not found: {file_path}") from e
        except Exception as e:
            raise Exception("Error reading CSV file") from e

        return cls(data=data)


@pydantic_dataclass
@dataclass
class SampleMetadata(Metadata):
    """Sample metadata class"""

@pydantic_dataclass
@dataclass
class ExperimentDesign(Metadata):
    """Experiment design class"""
    
    def filenames(self) -> List[str]:
        """Return the values under the 'filename' header.

        Looks for a column named 'filename' (case-insensitive) in the
        first row (header). Returns the entries from subsequent rows
        for that column. If header not found or data missing, returns [].
        """
        if not self.data:
            return []

        header = self.data[0]
        if not isinstance(header, list):
            return []

        # Find 'filename' column index (case-insensitive)
        try:
            filename_idx = next(
                i for i, h in enumerate(header) if isinstance(h, str) and h.lower() == "filename"
            )
        except StopIteration:
            return []

        filenames: List[str] = []
        for row in self.data[1:]:
            if not isinstance(row, list):
                continue
            if len(row) <= filename_idx:
                continue
            value = row[filename_idx]
            if isinstance(value, str) and value.strip():
                filenames.append(value)
        return filenames
