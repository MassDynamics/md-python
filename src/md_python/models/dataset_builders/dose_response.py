from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ..metadata import SampleMetadata
from ._base import BaseDatasetBuilder


def _dose_column_to_numbers(values: List[Any]) -> List[float]:
    """Convert a list of dose values (strings or numbers) to list of float."""
    result: List[float] = []
    for v in values:
        if v is None or (isinstance(v, str) and not str(v).strip()):
            result.append(0.0)
        elif isinstance(v, (int, float)):
            result.append(float(v))
        else:
            try:
                result.append(float(str(v).strip()))
            except ValueError:
                result.append(0.0)
    return result


@pydantic_dataclass
class DoseResponseDataset(BaseDatasetBuilder):
    """Builder for a dose response analysis dataset.

    Sends input_dataset_ids, name, job_slug, sample_names, and job_run_params.
    If sample_metadata is provided, job_run_params also includes experiment_design
    (column name -> list of values, with dose as numbers).
    """

    sample_names: List[str]
    control_samples: List[str]
    sample_metadata: Optional[SampleMetadata] = None
    dose_column: str = "dose"
    log_intensities: bool = True
    # Source-of-truth: data-set-service/src/flows/utils/type_defs.py:78 (DoseResponseParams)
    # has use_imputed_intensities default=False. Keep client default aligned.
    use_imputed_intensities: bool = False
    normalise: str = "none"
    span_rollmean_k: int = 1
    prop_required_in_protein: float = 0.5
    job_slug: str = "dose_response"

    def to_dataset(self) -> Dataset:
        job_run_params: Dict[str, Any] = {
            "control_samples": self.control_samples,
            "log_intensities": self.log_intensities,
            "use_imputed_intensities": self.use_imputed_intensities,
            "normalise": self.normalise,
            "span_rollmean_k": self.span_rollmean_k,
            "prop_required_in_protein": self.prop_required_in_protein,
        }
        if self.sample_metadata is not None:
            experiment_design: Dict[str, Any] = dict(self.sample_metadata.to_columns())
            if self.dose_column in experiment_design:
                experiment_design[self.dose_column] = _dose_column_to_numbers(
                    experiment_design[self.dose_column]
                )
            job_run_params["experiment_design"] = experiment_design

        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            sample_names=self.sample_names,
            job_run_params=job_run_params,
        )

    @classmethod
    def help(cls) -> str:
        """Return a human-readable description of parameters."""
        lines = [
            "DoseResponseDataset parameters:",
            "- input_dataset_ids (List[str]): non-empty list of input dataset UUIDs",
            "- dataset_name (str): name for the output dataset",
            "- sample_names (List[str]): sample names included in the analysis",
            "- control_samples (List[str]): sample names used as controls",
            "- log_intensities (bool): log-transform intensities (default True)",
            "- use_imputed_intensities (bool): use imputed intensities (default True)",
            "- normalise (str): normalisation method (default 'none')",
            "- span_rollmean_k (int): rolling mean span (default 1)",
            "- prop_required_in_protein (float): proportion required per protein 0–1 (default 0.5)",
            "- job_slug (str): backend job slug (default dose_response)",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not self.sample_names:
            raise ValueError("sample_names cannot be empty")
        if not self.control_samples:
            raise ValueError("control_samples cannot be empty")
        if not isinstance(self.control_samples, list):
            raise ValueError("control_samples must be a list")
        for s in self.control_samples:
            if s not in self.sample_names:
                raise ValueError(
                    f"control_samples must be a subset of sample_names; '{s}' not in sample_names"
                )
        if not 0 <= self.prop_required_in_protein <= 1:
            raise ValueError("prop_required_in_protein must be between 0 and 1")
        if self.span_rollmean_k < 1:
            raise ValueError("span_rollmean_k must be >= 1")
