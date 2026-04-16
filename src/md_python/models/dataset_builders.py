from abc import ABC, abstractmethod
from dataclasses import field
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from .dataset import Dataset
from .metadata import SampleMetadata

if TYPE_CHECKING:
    from ..base_client import BaseMDClient


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
class BaseDatasetBuilder(ABC):
    """Abstract base for dataset builders that produce Dataset objects.

    Shared parameters across dataset builders.
    """

    # Shared fields
    input_dataset_ids: List[str]
    dataset_name: str

    @abstractmethod
    def to_dataset(self) -> Dataset: ...

    @abstractmethod
    def validate(self) -> None:
        """Validate input fields; subclasses must implement."""
        ...

    def run(self, client: "BaseMDClient") -> str:
        """Create the dataset via the API and return the new dataset_id."""
        self.validate()
        return client.datasets.create(self.to_dataset())  # type: ignore[attr-defined, no-any-return]


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


@pydantic_dataclass
class NormalisationImputationDataset(BaseDatasetBuilder):
    """Builder for normalisation + imputation dataset.

    Required parameters are input datasets, output name, and two parameter blocks:
    - normalisation_methods: {"method": str, ...}
    - imputation_methods: {"method": str, ...}
    """

    normalisation_methods: Dict[str, Any]
    imputation_methods: Dict[str, Any]
    job_slug: str = "normalisation_imputation"

    def to_dataset(self) -> Dataset:
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params={
                "normalisation_methods": self.normalisation_methods,
                "imputation_methods": self.imputation_methods,
                "dataset_name": self.dataset_name,
            },
        )

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")

        if not isinstance(self.normalisation_methods, dict):
            raise ValueError("normalisation_methods must be a dictionary")
        if "method" not in self.normalisation_methods:
            raise ValueError("normalisation_methods must include 'method'")

        if not isinstance(self.imputation_methods, dict):
            raise ValueError("imputation_methods must be a dictionary")
        if "method" not in self.imputation_methods:
            raise ValueError("imputation_methods must include 'method'")


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
    use_imputed_intensities: bool = True
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


@pydantic_dataclass
class PairwiseComparisonDataset(BaseDatasetBuilder):
    """Builder for a pairwise comparison dataset with run support.

    Parameters
    ----------
    input_dataset_ids : List[str]
        List of dataset UUID strings that act as inputs to the job. Must be non-empty.
    dataset_name : str
        Friendly name for the output dataset.
    sample_metadata : SampleMetadata
        Sample metadata (columns -> values) used by downstream logic.
    condition_column : str
        Column in `sample_metadata` defining groups to compare (e.g. "condition").
    condition_comparisons : List[List[str]]
        List of pairwise comparisons, each item is [case, control]. Must be non-empty
        and every pair must contain exactly two elements.
    filter_valid_values_logic : str
        One of: "all conditions", "at least one condition", "full experiment".
        Controls how valid values are filtered before analysis. Default: "at least one condition".
    filter_values_criteria : Optional[Dict[str, Any]]
        Criteria dict controlling the filtering method. Supported forms:
        - {"method": "percentage", "filter_threshold_percentage": float in [0,1]}
        - {"method": "count", "filter_threshold_count": int >= 0}
        If omitted, defaults to {"method": "percentage", "filter_threshold_percentage": 0.5}.
    fit_separate_models : bool
        Whether to fit separate statistical models per comparison. Default: True.
    limma_trend : bool
        Whether to apply limma trend. Default: True.
    robust_empirical_bayes : bool
        Whether to apply robust empirical Bayes moderation. Default: True.
    control_variables : Optional[Dict[str, List[Dict[str, str]]]]
        Optional dictionary of control variables in the form:
        {"control_variables": [{"column": str, "type": "numerical"|"categorical"}, ...]}
    entity_type : str
        One of: "protein", "peptide". Default: "protein".
    job_slug : str
        Job slug for the backend flow. Default: "pairwise_comparison".
    """

    # Shared fields inherited: input_dataset_ids, dataset_name, job_slug
    sample_metadata: SampleMetadata
    condition_column: str
    condition_comparisons: List[List[str]]
    filter_values_criteria: Dict[str, Any] = field(
        default_factory=lambda: {
            "method": "percentage",
            "filter_threshold_percentage": 0.5,
        }
    )
    filter_valid_values_logic: str = "at least one condition"
    log_intensities: bool = True
    use_imputed_intensities: bool = True
    fit_separate_models: bool = True
    limma_trend: bool = True
    robust_empirical_bayes: bool = True
    control_variables: Optional[Dict[str, List[Dict[str, str]]]] = None
    entity_type: str = "protein"
    job_slug: str = "pairwise_comparison"

    @staticmethod
    def pairwise_vs_control(
        sample_metadata: SampleMetadata, column: str, control: str
    ) -> List[List[str]]:
        """Generate [case, control] pairs for every unique value in *column* vs *control*.

        Preserves first-seen order, ignores empty values, excludes the control itself.
        """
        cols = sample_metadata.to_columns()
        if column not in cols:
            raise ValueError(f"Column '{column}' not found in sample metadata")
        seen: set = set()
        ordered: List[str] = []
        for value in cols[column]:
            if value and value not in seen:
                seen.add(value)
                ordered.append(value)
        return [[value, control] for value in ordered if value != control]

    @staticmethod
    def all_pairwise_comparisons(
        sample_metadata: SampleMetadata, column: str
    ) -> List[List[str]]:
        """Generate all unique [case, control] pairs from every combination of distinct
        values in *column*, preserving first-seen order.

        Each unordered pair (a, b) appears once as [b, a] where a appears first in the data.
        """
        from itertools import combinations

        cols = sample_metadata.to_columns()
        if column not in cols:
            raise ValueError(f"Column '{column}' not found in sample metadata")
        seen: set = set()
        ordered: List[str] = []
        for value in cols[column]:
            if value and value not in seen:
                seen.add(value)
                ordered.append(value)
        return [[b, a] for a, b in combinations(ordered, 2)]

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
                "filter_values_criteria": self.filter_values_criteria,
                "log_intensities": self.log_intensities,
                "use_imputed_intensities": self.use_imputed_intensities,
                "fit_separate_models": self.fit_separate_models,
                "limma_trend": self.limma_trend,
                "robust_empirical_bayes": self.robust_empirical_bayes,
                "control_variables": self.control_variables,
                "entity_type": self.entity_type,
            },
        )

    @classmethod
    def help(cls) -> str:
        """Return a human-readable description of parameters and valid values."""
        lines = [
            "PairwiseComparisonDataset parameters:",
            "- input_dataset_ids (List[str]): non-empty list of UUID strings",
            "- dataset_name (str): name for the output dataset",
            "- sample_metadata (SampleMetadata): metadata table used for grouping",
            "- condition_column (str): column in sample_metadata defining groups",
            "- condition_comparisons (List[List[str]]): list of [case, control] pairs",
            "- filter_valid_values_logic (str): one of {all conditions, at least one condition, full experiment}",
            "- filter_values_criteria (dict): {method: percentage|count, filter_threshold_percentage:[0,1] or filter_threshold_count:>=0}",
            "- fit_separate_models (bool): whether to fit separate models (default True)",
            "- limma_trend (bool): apply limma trend (default True)",
            "- robust_empirical_bayes (bool): apply robust EB moderation (default True)",
            "- control_variables (dict): {'control_variables': [{column: str, type: numerical|categorical}, ...]} (optional)",
            "- entity_type (str): protein|peptide (default protein)",
            "- job_slug (str): backend job slug (default pairwise_comparison)",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")

        if not self.dataset_name:
            raise ValueError("dataset_name is required")

        if not self.condition_column:
            raise ValueError("condition_column is required")

        if not self.condition_comparisons:
            raise ValueError("condition_comparisons cannot be empty")
        if not isinstance(self.condition_comparisons, list) or not all(
            isinstance(x, list) for x in self.condition_comparisons
        ):
            raise ValueError("condition_comparisons must be a list of lists")
        if not all(len(x) == 2 for x in self.condition_comparisons):
            raise ValueError("each condition comparison must have exactly 2 elements")

        # booleans
        if not isinstance(self.fit_separate_models, bool):
            raise ValueError("fit_separate_models must be a bool")
        if not isinstance(self.limma_trend, bool):
            raise ValueError("limma_trend must be a bool")
        if not isinstance(self.robust_empirical_bayes, bool):
            raise ValueError("robust_empirical_bayes must be a bool")

        # entity type
        if self.entity_type not in {"protein", "peptide"}:
            raise ValueError("entity_type must be one of: protein, peptide")

        if self.filter_valid_values_logic not in [
            "all conditions",
            "at least one condition",
            "full experiment",
        ]:
            raise ValueError(
                "filter_value_logic must be one of: all conditions, at least one condition, full experiment"
            )

        if self.filter_values_criteria is not None:
            crit = self.filter_values_criteria
            if not hasattr(crit, "get"):
                raise ValueError("filter_values_criteria must be a dict-like object")

            method = crit.get("method")

            if method not in ["percentage", "count"]:
                raise ValueError(
                    "filter_values_criteria method must be one of: percentage, count"
                )
            elif method == "percentage":
                pct = crit.get("filter_threshold_percentage")
                if pct is not None and (pct < 0 or pct > 1):
                    raise ValueError(
                        "filter_values_criteria filter_threshold_percentage must be between 0 and 1"
                    )
            elif method == "count":
                cnt = crit.get("filter_threshold_count")
                if cnt is not None and cnt < 0:
                    raise ValueError(
                        "filter_values_criteria filter_threshold_count must be greater than 0"
                    )

        if self.control_variables is not None:
            if not isinstance(self.control_variables, dict):
                raise ValueError("control_variables must be a dictionary")
            items = self.control_variables.get("control_variables")
            if items is None:
                raise ValueError(
                    "control_variables must include 'control_variables' list"
                )
            if not isinstance(items, list):
                raise ValueError(
                    "control_variables['control_variables'] must be a list"
                )
            for _, item in enumerate(items):
                if not isinstance(item, dict):
                    raise ValueError("each control variable must be a dictionary")
                if "column" not in item or "type" not in item:
                    raise ValueError(
                        "each control variable must include 'column' and 'type'"
                    )
                column = item.get("column")
                ctype = item.get("type")
                if not isinstance(column, str) or not column.strip():
                    raise ValueError(
                        "control variable 'column' must be a non-empty string"
                    )
                if ctype not in {"numerical", "categorical"}:
                    raise ValueError(
                        "control variable 'type' must be one of: numerical, categorical"
                    )
