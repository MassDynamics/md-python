from typing import List, Dict, Any, Optional, TYPE_CHECKING
from uuid import UUID
from dataclasses import dataclass

from md_python.models import Dataset
from md_python.models import SampleMetadata

from md_python.client import MDClient


def create_pairwise_comparison_dataset(
    input_dataset_ids: List[str],
    dataset_name: str,
    sample_metadata: SampleMetadata,
    condition_column: str,
    condition_comparisons: List[List[str]],
    filter_valid_values_logic: str = "at least one condition",  # ["all conditions", "at least one condition", "full experiment"]
    filter_values_criteria: Dict[str, Any] = {"method": "percentage", "filter_threshold_percentage": 0.5},  # 'count', 'filter_threshold_count'
    fit_separate_models: bool = True,
    limma_trend: bool = True,
    robust_empirical_bayes: bool = True,
    control_variables: List[Dict[str, str]] = None,
    entity_type: str = "protein",
    job_slug: str = "pairwise_comparison",
) -> Dataset:
    """Build a Dataset for the pairwise comparison job (no network calls)."""
    return Dataset(
        input_dataset_ids=[UUID(x) for x in input_dataset_ids],
        name=dataset_name,
        job_slug=job_slug,
        job_run_params={
            "condition_column": condition_column,
            "condition_comparisons": {"condition_comparison_pairs": condition_comparisons},
            "experiment_design": sample_metadata.to_columns(),
            "filter_valid_values_logic": filter_valid_values_logic,
            "filter_values_criteria": filter_values_criteria,
            "fit_separate_models": fit_separate_models,
            "limma_trend": limma_trend,
            "robust_empirical_bayes": robust_empirical_bayes,
            "control_variables": control_variables,
            "entity_type": entity_type,
        },
    )



@dataclass
class PairwiseComparisonDataset:
    """Builder for a pairwise comparison dataset with run support."""
    input_dataset_ids: List[str]
    dataset_name: str
    sample_metadata: SampleMetadata
    condition_column: str
    condition_comparisons: List[List[str]]
    filter_valid_values_logic: str = "at least one condition"
    filter_values_criteria: Dict[str, Any] = None
    fit_separate_models: bool = True
    limma_trend: bool = True
    robust_empirical_bayes: bool = True
    control_variables: Optional[List[Dict[str, str]]] = None
    entity_type: str = "protein"
    job_slug: str = "pairwise_comparison"

    def to_dataset(self) -> Dataset:
        """Return the Dataset object constructed from the stored parameters."""
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params={
                "condition_column": self.condition_column,
                "condition_comparisons": {"condition_comparison_pairs": self.condition_comparisons},
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

    def run(self, client: MDClient) -> str:
        """Create the dataset via the API and return the new dataset_id."""
        return client.datasets.create(self.to_dataset())

