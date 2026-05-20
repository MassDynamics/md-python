from typing import Any, Dict, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ._base import BaseDatasetBuilder


@pydantic_dataclass
class MinimalDataset(BaseDatasetBuilder):
    """Builder for a minimal dataset (name, inputs, job slug only)."""

    job_slug: str
    job_run_params: Optional[Dict[str, Any]] = None

    def to_dataset(self) -> Dataset:
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params=self.job_run_params or {},
        )

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not self.job_slug:
            raise ValueError("job_slug is required")
