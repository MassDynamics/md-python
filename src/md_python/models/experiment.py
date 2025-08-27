"""
Experiment model for create, update, and retrieval operations
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from pydantic.dataclasses import dataclass as pydantic_dataclass
from datetime import datetime
from uuid import UUID
from .metadata import SampleMetadata, ExperimentDesign


@pydantic_dataclass
@dataclass
class Experiment:
    """Experiment model that can be used for create, update, and retrieval operations"""

    name: str
    source: str
    s3_bucket: str
    filenames: List[str]
    id: Optional[UUID] = None
    description: Optional[str] = None
    experiment_design: Optional[ExperimentDesign] = None
    labelling_method: Optional[str] = None
    s3_prefix: Optional[str] = None
    sample_metadata: Optional[SampleMetadata] = None
    created_at: Optional[datetime] = None
    status: Optional[str] = None

    def __str__(self) -> str:
        """Return a readable string representation of the experiment"""
        lines = [f"Experiment: {self.name}"]
        if self.id:
            lines.append(f"ID: {self.id}")
        if self.description:
            lines.append(f"Description: {self.description}")
        lines.append(f"Source: {self.source}")
        if self.status:
            lines.append(f"Status: {self.status}")
        if self.labelling_method:
            lines.append(f"Labelling Method: {self.labelling_method}")
        if self.created_at:
            lines.append(f"Created: {self.created_at}")
        if self.s3_bucket:
            lines.append(f"S3 Bucket: {self.s3_bucket}")
        if self.s3_prefix:
            lines.append(f"S3 Prefix: {self.s3_prefix}")
        if self.filenames:
            lines.append(f"Files: {len(self.filenames)} files")
        if self.experiment_design:
            lines.append("Experiment Design:")
            lines.append(str(self.experiment_design))
        if self.sample_metadata:
            lines.append("Sample Metadata:")
            lines.append(str(self.sample_metadata))

        return "\n".join(lines)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Experiment":
        """
        Create Experiment object from JSON response data

        Args:
            data: Dictionary containing experiment data from API response

        Returns:
            Experiment object with data from JSON
        """

        # Extract created_at with proper type checking
        created_at_raw = data.get("created_at")
        created_at = None
        if created_at_raw is not None and isinstance(created_at_raw, str):
            created_at = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))

        return cls(
            id=UUID(data.get("id")) if data.get("id") else None,
            name=data.get("name", ""),
            description=data.get("description"),
            labelling_method=data.get("labelling_method"),
            source=data.get("source", ""),
            s3_bucket=data.get("s3_bucket", ""),
            s3_prefix=data.get("s3_prefix"),
            filenames=data.get("filenames", []),
            experiment_design=(
                ExperimentDesign(data=data.get("experiment_design", []))
                if data.get("experiment_design") is not None
                else None
            ),
            sample_metadata=(
                SampleMetadata(data=data.get("sample_metadata", []))
                if data.get("sample_metadata") is not None
                else None
            ),
            created_at=created_at,
            status=data.get("status"),
        )
