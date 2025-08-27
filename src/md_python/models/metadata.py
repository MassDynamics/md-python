"""
Metadata model for handling 2D array data with CSV import capabilities
"""

import csv
from typing import List
from abc import ABC
from dataclasses import dataclass
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
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        except Exception as e:
            raise Exception(f"Error reading CSV file: {e}")

        return cls(data=data)


@pydantic_dataclass
@dataclass
class SampleMetadata(Metadata):
    """Sample metadata class"""

    pass


@pydantic_dataclass
@dataclass
class ExperimentDesign(Metadata):
    """Experiment design class"""

    pass
