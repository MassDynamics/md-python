from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, TYPE_CHECKING, Dict, Any, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from .dataset import Dataset
from .metadata import SampleMetadata
if TYPE_CHECKING:
    from ..client import MDClient


class BaseDatasetBuilder(ABC):
    """Abstract base for dataset builders that produce Dataset objects."""

    @abstractmethod
    def to_dataset(self) -> Dataset:  # pragma: no cover - interface only
        ...

    @abstractmethod
    def validate(self) -> None:  # pragma: no cover - interface only
        """Validate input fields; raise ValueError with a concise message on failure."""
        ...

    def run(self, client: "MDClient") -> str:
        """Create the dataset via the API and return the new dataset_id."""
        self.validate()
        return client.datasets.create(self.to_dataset())


@pydantic_dataclass
@dataclass
class MinimalDataset(BaseDatasetBuilder):
    """Builder for a minimal dataset (name, inputs, job slug only)."""

    input_dataset_ids: List[str]
    dataset_name: str
    job_slug: str

    def to_dataset(self) -> Dataset:
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params={},
        )

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not self.job_slug:
            raise ValueError("job_slug is required")


@pydantic_dataclass
@dataclass
class PairwiseComparisonDataset(BaseDatasetBuilder):
    """Builder for a pairwise comparison dataset with run support."""

    input_dataset_ids: List[str]
    dataset_name: str
    sample_metadata: SampleMetadata
    condition_column: str
    condition_comparisons: List[List[str]]
    filter_valid_values_logic: str = "at least one condition"
    filter_values_criteria: Optional[Dict[str, Any]] = None
    fit_separate_models: bool = True
    limma_trend: bool = True
    robust_empirical_bayes: bool = True
    control_variables: Optional[List[Dict[str, str]]] = None
    entity_type: str = "protein"
    job_slug: str = "pairwise_comparison"

    def to_dataset(self) -> Dataset:
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params={
                "condition_column": self.condition_column,
                "condition_comparisons": {
                    "condition_comparison_pairs": self.condition_comparisons
                },
                "experiment_design": self.sample_metadata.to_columns(),
                "filter_valid_values_logic": self.filter_valid_values_logic,
                "filter_values_criteria": (
                    self.filter_values_criteria
                    if self.filter_values_criteria is not None
                    else {"method": "percentage", "filter_threshold_percentage": 0.5}
                ),
                "fit_separate_models": self.fit_separate_models,
                "limma_trend": self.limma_trend,
                "robust_empirical_bayes": self.robust_empirical_bayes,
                "control_variables": self.control_variables,
                "entity_type": self.entity_type,
            },
        )

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not self.condition_column:
            raise ValueError("condition_column is required")
        if not self.condition_comparisons:
            raise ValueError("condition_comparisons cannot be empty")


