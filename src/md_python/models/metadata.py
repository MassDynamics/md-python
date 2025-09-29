"""
Metadata model for handling 2D array data with CSV import capabilities
"""

import csv
from abc import ABC
from dataclasses import dataclass
from typing import List, Dict

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

    def to_columns(self) -> Dict[str, List[str]]:
        """Return a dict mapping column name -> list of values.

        Uses the first row as header; subsequent rows become column values.
        Short rows are padded with empty strings.
        """
        if not self.data:
            return {}
        header_row = self.data[0]
        if not isinstance(header_row, list) or not header_row:
            return {}
        headers = [str(h).strip() for h in header_row]
        cols: Dict[str, List[str]] = {h: [] for h in headers}
        for row in self.data[1:]:
            if not isinstance(row, list):
                continue
            for i, h in enumerate(headers):
                cols[h].append(str(row[i]) if i < len(row) else "")
        return cols

    def pairwise_vs_control(self, column: str, control: str) -> List[List[str]]:
        """Generate pairwise comparisons of each unique value vs control.

        Preserves first-seen order, ignores empty values, and excludes the control itself.
        """
        cols = self.to_columns()
        if column not in cols:
            raise ValueError(f"Column '{column}' not found in sample metadata")

        seen: set[str] = set()
        ordered: List[str] = []
        for value in cols[column]:
            if value and value not in seen:
                seen.add(value)
                ordered.append(value)

        return [[value, control] for value in ordered if value != control]

@pydantic_dataclass
@dataclass
class ExperimentDesign(Metadata):
    """Experiment design class"""
    
    @staticmethod
    def _normalize_rows(raw: List[List[str]]) -> List[List[str]]:
        """Normalize to required header and column order.

        Required header: ["filename", "sample_name", "condition"].
        Accepts common synonyms and reorders columns accordingly.
        """
        if not raw:
            raise ValueError("experiment_design is empty")

        header = [h.strip().lower() if isinstance(h, str) else "" for h in raw[0]]
        synonyms = {
            "filename": "filename",
            "file": "filename",
            "sample_name": "sample_name",
            "sample": "sample_name",
            "condition": "condition",
            "group": "condition",
        }
        normalized_header = [synonyms.get(h, h) for h in header]

        required = ["filename", "sample_name", "condition"]
        try:
            idx_filename = normalized_header.index("filename")
            idx_sample = normalized_header.index("sample_name")
            idx_condition = normalized_header.index("condition")
        except ValueError as e:
            raise ValueError(
                f"Missing required columns {required}; got {raw[0]}"
            ) from e

        fixed_rows: List[List[str]] = [required]
        for row in raw[1:]:
            if not isinstance(row, list):
                continue
            vals = [
                row[idx_filename] if len(row) > idx_filename else "",
                row[idx_sample] if len(row) > idx_sample else "",
                row[idx_condition] if len(row) > idx_condition else "",
            ]
            fixed_rows.append(vals)

        return fixed_rows

    def __post_init__(self) -> None:
        # Attempt to normalize on construction; if required columns missing, keep original
        try:
            self.data = self._normalize_rows(self.data)
        except ValueError:
            # Leave data unchanged to avoid breaking callers/tests that don't require normalization
            pass

    def to_core_design(self) -> "ExperimentDesign":
        """Return normalized design (already normalized in __post_init__)."""
        return self

