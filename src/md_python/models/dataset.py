"""
Dataset model for create, update, and retrieval operations
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass
@dataclass
class Dataset:
    """Dataset model that can be used for create, update, and retrieval operations"""

    input_dataset_ids: List[UUID]
    name: str
    job_slug: str
    job_run_params: Dict[str, Any]
    type: Optional[str] = None
    state: Optional[str] = None
    id: Optional[UUID] = None
    sample_names: Optional[List[str]] = None
    job_run_start_time: Optional[datetime] = None

    def __str__(self) -> str:
        """Return a readable string representation of the dataset"""
        lines = [f"Name: {self.name}"]
        if self.id:
            lines.append(f"ID: {self.id}")
        if self.job_slug:
            lines.append(f"Job Slug: {self.job_slug}")
        if self.input_dataset_ids:
            lines.append(
                f"Input Dataset IDs: {[str(did) for did in self.input_dataset_ids]}"
            )
        if self.sample_names:
            lines.append(f"Sample Names: {self.sample_names}")
        if self.job_run_params:
            lines.append(f"Job Run Params: {self.job_run_params}")
        if self.job_run_start_time:
            lines.append(f"Job Run Start Time: {self.job_run_start_time}")

        return "\n".join(lines)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Dataset":
        """
        Create Dataset object from JSON response data

        Args:
            data: Dictionary containing dataset data from API response

        Returns:
            Dataset object with data from JSON
        """

        # Extract job_run_start_time with proper type checking
        job_run_start_time_raw = data.get("job_run_start_time")
        job_run_start_time = None
        if job_run_start_time_raw is not None and isinstance(
            job_run_start_time_raw, str
        ):
            job_run_start_time = datetime.fromisoformat(
                job_run_start_time_raw.replace("Z", "+00:00")
            )

        return cls(
            id=UUID(data.get("id")) if data.get("id") else None,
            input_dataset_ids=[UUID(did) for did in data.get("input_dataset_ids", [])],
            name=data.get("name", ""),
            job_slug=data.get("job_slug", ""),
            sample_names=data.get("sample_names"),
            job_run_params=data.get("job_run_params", {}),
            type=data.get("type"),
            state=data.get("state"),
            job_run_start_time=job_run_start_time,
        )
