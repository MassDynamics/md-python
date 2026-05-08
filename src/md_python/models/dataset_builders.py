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


_ENTITY_TYPES = {"protein", "peptide", "gene"}

_PROTEOMICS_NORMALISATION_METHODS = {
    "skip",
    "median",
    "quantile",
    "sum",
    "batch correction",
}
_GENE_NORMALISATION_METHODS = {
    "skip",
    "median",
    "quantile",
    "sum",
    "batch correction",
    "cpm",
}

_IMPUTATION_METHODS = {
    "skip",
    "mnar",
    "global_median",
    "median_by_entity",
    "knn",
    "knn_tn",
    "set to constant",
    "set to missing",
    "mindet",
}

_PROTEIN_FILTRATION_METHODS = {"skip", "by missing values"}
_PEPTIDE_FILTRATION_METHODS = {
    "skip",
    "by missing values",
    "by ptm localization probability",
}
_GENE_FILTRATION_METHODS = {"skip", "by minimum abundance"}

_BATCH_CORRECTION_TECHNIQUES_PROTEOMICS = {
    "limma remove batch effect",
    "combat",
}
_BATCH_CORRECTION_TECHNIQUES_GENE = {
    "limma remove batch effect",
    "combat",
    "combat seq",
}

_FILTER_VALID_VALUES_CRITERIA = {"percentage", "count"}
_FILTER_VALID_VALUES_LOGIC = {
    "all conditions",
    "at least one condition",
    "full experiment",
}

_KNN_TN_DISTANCE = {"truncation", "correlation"}
_KNN_WEIGHTS = {"uniform", "distance"}

# Legacy underscored input values are accepted for backward compatibility and
# normalised to the converter-canonical (spaced) form on the wire.
_METHOD_ALIAS_MAP: Dict[str, str] = {
    "batch_correction": "batch correction",
    "minimum_abundance": "by minimum abundance",
    "by_minimum_abundance": "by minimum abundance",
    "ptm_localization_probability": "by ptm localization probability",
    "by_ptm_localization_probability": "by ptm localization probability",
    "by_missing_values": "by missing values",
    "limma_remove_batch_effect": "limma remove batch effect",
    "combat_seq": "combat seq",
}


def _canon_method(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return _METHOD_ALIAS_MAP.get(value, value)


def _normalisation_methods_key(entity_type: str) -> str:
    return (
        "normalisation_methods_gene"
        if entity_type == "gene"
        else "normalisation_methods_proteomics"
    )


def _filtration_methods_key(entity_type: str) -> str:
    return f"filtration_methods_{entity_type}"


def _batch_correction_technique_key(entity_type: str) -> str:
    return (
        "batch_correction_technique_gene"
        if entity_type == "gene"
        else "batch_correction_technique_proteomics"
    )


@pydantic_dataclass
class NormalisationImputationDataset(BaseDatasetBuilder):
    """Builder for the normalisation + imputation + filtration pipeline.

    The job runs three steps in fixed order: filtration -> normalisation -> imputation.
    Each step can be skipped independently. The output dataset is of type INTENSITY.

    Required: ``input_dataset_ids``, ``dataset_name``, ``normalisation_method``,
    ``imputation_method``. ``entity_type`` defaults to ``"protein"``.

    See :meth:`help` for the full list of supported methods, per-method parameters,
    and entity-type constraints.
    """

    normalisation_method: str
    imputation_method: str
    entity_type: str = "protein"
    filtration_method: Optional[str] = None

    # Normalisation knobs
    include_imputed_values: Optional[bool] = None
    median_normalisation_centre_at_zero: Optional[bool] = None
    prior_count: Optional[float] = None
    batch_correction_technique: Optional[str] = None
    batch_variables: Optional[List[Any]] = None
    batch_variable_combat: Optional[str] = None
    reference_batch_combat: Optional[str] = None
    mean_only: Optional[bool] = None
    design_variables: Optional[List[Any]] = None
    experiment_design: Optional[Dict[str, Any]] = None

    # Imputation knobs
    std_position: Optional[float] = None
    std_width: Optional[float] = None
    n_neighbors: Optional[int] = None
    weights: Optional[str] = None
    knn_tn_k: Optional[int] = None
    knn_tn_distance: Optional[str] = None
    constant_value: Optional[float] = None
    q: Optional[float] = None

    # Filtration knobs
    threshold: Optional[float] = None
    minimum_abundance_threshold: Optional[float] = None
    filter_valid_values_criteria: Optional[str] = None
    filter_threshold_proportion: Optional[float] = None
    filter_threshold_count: Optional[int] = None
    filter_valid_values_logic: Optional[str] = None
    filter_based_on_condition: Optional[str] = None

    extra_params: Optional[Dict[str, Any]] = None
    job_slug: str = "normalisation_imputation"

    @classmethod
    def filter_only(
        cls,
        *,
        input_dataset_ids: List[str],
        dataset_name: str,
        entity_type: str,
        filtration_method: str,
        **kwargs: Any,
    ) -> "NormalisationImputationDataset":
        """Build an NI dataset that runs filtration only.

        Sets ``normalisation_method='skip'`` and ``imputation_method='skip'``.
        The output is still an INTENSITY dataset (the converter registers the NI
        flow with ``run_type=DatasetType.INTENSITY``).
        """
        return cls(
            input_dataset_ids=input_dataset_ids,
            dataset_name=dataset_name,
            entity_type=entity_type,
            normalisation_method="skip",
            imputation_method="skip",
            filtration_method=filtration_method,
            **kwargs,
        )

    @classmethod
    def help(cls) -> str:  # noqa: C901  (deliberately long-form documentation)
        """Return a human-readable description of supported methods and params."""
        lines = [
            "NormalisationImputationDataset parameters:",
            "  Required:",
            "    input_dataset_ids (List[str]), dataset_name (str),",
            "    normalisation_method (str), imputation_method (str)",
            "  Optional core:",
            "    entity_type: protein | peptide | gene  (default protein)",
            "    filtration_method: see entity-type table below  (default skip)",
            "    extra_params (dict): forward-compat escape hatch; merged LAST so",
            "      caller-supplied values override typed kwargs.",
            "",
            "Allowed normalisation methods (entity_type-specific):",
            "  protein, peptide: skip | median | quantile | sum | batch correction",
            "  gene:             skip | median | quantile | sum | batch correction | cpm",
            "Method-specific params:",
            "  median:   median_normalisation_centre_at_zero (bool, default True),",
            "            include_imputed_values (bool, default False)",
            "  quantile, sum, batch correction:  include_imputed_values",
            "  cpm (gene only):   prior_count (float, default 0)",
            "  batch correction:  batch_correction_technique (REQUIRED) -",
            "    limma remove batch effect: batch_variables (list),",
            "       design_variables (list), experiment_design (dict)",
            "    combat: batch_variable_combat (str), design_variables (list),",
            "       experiment_design (dict), mean_only (bool, default False),",
            "       reference_batch_combat (str, optional)",
            "    combat seq (gene only): batch_variable_combat, design_variables,",
            "       experiment_design",
            "",
            "Allowed imputation methods:",
            "  skip | mnar | global_median | median_by_entity | knn | knn_tn |",
            "  set to constant | set to missing | mindet",
            "Method-specific params:",
            "  mnar:             std_position (default 1.8), std_width (default 0.3)",
            "  knn:              n_neighbors (1-10, default 3),",
            "                    weights (uniform|distance, default uniform)",
            "  knn_tn:           knn_tn_k (1-10, default 5),",
            "                    knn_tn_distance (truncation|correlation,",
            "                                     default truncation)",
            "  set to constant:  constant_value (default 0)",
            "  mindet:           q (0-0.5, default 0.01)",
            "",
            "Allowed filtration methods (entity_type-specific):",
            "  protein:  skip | by missing values",
            "  peptide:  skip | by missing values | by ptm localization probability",
            "  gene:     skip | by minimum abundance",
            "Method-specific params:",
            "  by ptm localization probability:  threshold (0-1)",
            "  by minimum abundance:             minimum_abundance_threshold (0-100),",
            "                                    + shared filter block below",
            "  by missing values:                shared filter block below",
            "Shared filter block (by missing values, by minimum abundance):",
            "  filter_valid_values_criteria: percentage | count",
            "  filter_threshold_proportion (0-1, used when criteria=percentage)",
            "  filter_threshold_count (>=1, used when criteria=count)",
            "  filter_valid_values_logic: all conditions | at least one condition |",
            "                              full experiment",
            "  filter_based_on_condition (column name, required for the first two",
            "                             logic values)",
            "  experiment_design (dict, required)",
            "",
            "Filter-only pattern:",
            "  Use NormalisationImputationDataset.filter_only(...) to run filtration",
            "  with normalisation=skip and imputation=skip. Output is still INTENSITY.",
            "",
            "Legacy underscored values (e.g. 'batch_correction', 'minimum_abundance',",
            "'ptm_localization_probability', 'by_missing_values') are accepted on",
            "input and normalised to the canonical spaced form on the wire.",
        ]
        return "\n".join(lines)

    def to_dataset(self) -> Dataset:
        norm = _canon_method(self.normalisation_method) or "skip"
        imp = _canon_method(self.imputation_method) or "skip"
        filt = _canon_method(self.filtration_method) or "skip"
        technique = _canon_method(self.batch_correction_technique)

        params: Dict[str, Any] = {
            "entity_type": self.entity_type,
            _normalisation_methods_key(self.entity_type): norm,
            _filtration_methods_key(self.entity_type): filt,
            "imputation_methods": imp,
        }

        # Normalisation method-specific params
        if norm == "median":
            params["median_normalisation_centre_at_zero"] = (
                True
                if self.median_normalisation_centre_at_zero is None
                else self.median_normalisation_centre_at_zero
            )
            params["include_imputed_values"] = (
                False
                if self.include_imputed_values is None
                else self.include_imputed_values
            )
        elif norm in {"quantile", "sum", "batch correction"}:
            params["include_imputed_values"] = (
                False
                if self.include_imputed_values is None
                else self.include_imputed_values
            )
        if norm == "cpm":
            params["prior_count"] = 0 if self.prior_count is None else self.prior_count

        if norm == "batch correction" and technique is not None:
            params[_batch_correction_technique_key(self.entity_type)] = technique
            if technique == "limma remove batch effect":
                if self.batch_variables is not None:
                    params["batch_variables"] = self.batch_variables
            elif technique in {"combat", "combat seq"}:
                if self.batch_variable_combat is not None:
                    params["batch_variable_combat"] = self.batch_variable_combat
                if technique == "combat":
                    params["mean_only"] = (
                        False if self.mean_only is None else self.mean_only
                    )
                    if self.reference_batch_combat is not None:
                        params["reference_batch_combat"] = self.reference_batch_combat
            if self.design_variables is not None:
                params["design_variables"] = self.design_variables

        # Imputation method-specific params
        if imp == "mnar":
            params["std_position"] = (
                1.8 if self.std_position is None else self.std_position
            )
            params["std_width"] = 0.3 if self.std_width is None else self.std_width
        elif imp == "knn":
            params["n_neighbors"] = 3 if self.n_neighbors is None else self.n_neighbors
            params["weights"] = "uniform" if self.weights is None else self.weights
        elif imp == "knn_tn":
            params["knn_tn_k"] = 5 if self.knn_tn_k is None else self.knn_tn_k
            params["knn_tn_distance"] = (
                "truncation" if self.knn_tn_distance is None else self.knn_tn_distance
            )
        elif imp == "set to constant":
            params["constant_value"] = (
                0 if self.constant_value is None else self.constant_value
            )
        elif imp == "mindet":
            params["q"] = 0.01 if self.q is None else self.q

        # Filtration method-specific params
        if filt == "by ptm localization probability":
            if self.threshold is not None:
                params["threshold"] = self.threshold
        elif filt == "by minimum abundance":
            if self.minimum_abundance_threshold is not None:
                params["minimum_abundance_threshold"] = self.minimum_abundance_threshold
        if filt in {"by missing values", "by minimum abundance"}:
            if self.filter_valid_values_criteria is not None:
                params["filter_valid_values_criteria"] = (
                    self.filter_valid_values_criteria
                )
            if self.filter_threshold_proportion is not None:
                params["filter_threshold_proportion"] = self.filter_threshold_proportion
            if self.filter_threshold_count is not None:
                params["filter_threshold_count"] = self.filter_threshold_count
            if self.filter_valid_values_logic is not None:
                params["filter_valid_values_logic"] = self.filter_valid_values_logic
            if self.filter_based_on_condition is not None:
                params["filter_based_on_condition"] = self.filter_based_on_condition

        # experiment_design lives at top level when batch correction or
        # conditional filtration is active.
        if self.experiment_design is not None:
            params["experiment_design"] = self.experiment_design

        # Caller's extra_params win — locks current semantics.
        if self.extra_params:
            params.update(self.extra_params)

        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params=params,
        )

    def validate(self) -> None:  # noqa: C901
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if self.entity_type not in _ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of: {sorted(_ENTITY_TYPES)}")

        norm = _canon_method(self.normalisation_method)
        imp = _canon_method(self.imputation_method)
        filt = _canon_method(self.filtration_method)
        technique = _canon_method(self.batch_correction_technique)

        allowed_norm = (
            _GENE_NORMALISATION_METHODS
            if self.entity_type == "gene"
            else _PROTEOMICS_NORMALISATION_METHODS
        )
        if norm not in allowed_norm:
            raise ValueError(
                f"normalisation_method '{self.normalisation_method}' not allowed "
                f"for entity_type='{self.entity_type}'. "
                f"Allowed: {sorted(allowed_norm)}"
            )

        if imp not in _IMPUTATION_METHODS:
            raise ValueError(
                f"imputation_method '{self.imputation_method}' is invalid. "
                f"Allowed: {sorted(_IMPUTATION_METHODS)}"
            )

        allowed_filt = {
            "protein": _PROTEIN_FILTRATION_METHODS,
            "peptide": _PEPTIDE_FILTRATION_METHODS,
            "gene": _GENE_FILTRATION_METHODS,
        }[self.entity_type]
        if filt is not None and filt not in allowed_filt:
            raise ValueError(
                f"filtration_method '{self.filtration_method}' not allowed "
                f"for entity_type='{self.entity_type}'. "
                f"Allowed: {sorted(allowed_filt)}"
            )

        # Batch correction
        if norm == "batch correction":
            if technique is None:
                raise ValueError(
                    "batch_correction_technique is required when "
                    "normalisation_method='batch correction'"
                )
            allowed_tech = (
                _BATCH_CORRECTION_TECHNIQUES_GENE
                if self.entity_type == "gene"
                else _BATCH_CORRECTION_TECHNIQUES_PROTEOMICS
            )
            if technique not in allowed_tech:
                raise ValueError(
                    f"batch_correction_technique '{self.batch_correction_technique}' "
                    f"not allowed for entity_type='{self.entity_type}'. "
                    f"Allowed: {sorted(allowed_tech)}"
                )
            if technique == "limma remove batch effect":
                if not self.batch_variables:
                    raise ValueError(
                        "batch_variables (non-empty list) is required for "
                        "limma remove batch effect"
                    )
            elif technique in {"combat", "combat seq"}:
                if (
                    self.batch_variable_combat is None
                    or not str(self.batch_variable_combat).strip()
                ):
                    raise ValueError(
                        "batch_variable_combat (non-empty str) is required for "
                        f"{technique}"
                    )
                if technique == "combat seq":
                    if self.mean_only is not None:
                        raise ValueError("mean_only is not valid for combat seq")
                    if self.reference_batch_combat is not None:
                        raise ValueError(
                            "reference_batch_combat is not valid for combat seq"
                        )
            if self.experiment_design is None:
                raise ValueError(
                    "experiment_design is required when normalisation_method="
                    "'batch correction'"
                )

        # Imputation per-method param validation
        if imp == "mnar":
            if self.std_position is not None and not 0 <= self.std_position <= 3:
                raise ValueError("std_position must be between 0 and 3")
            if self.std_width is not None and not 0 <= self.std_width <= 1:
                raise ValueError("std_width must be between 0 and 1")
        if imp == "knn":
            if self.n_neighbors is not None and not 1 <= self.n_neighbors <= 10:
                raise ValueError("n_neighbors must be between 1 and 10")
            if self.weights is not None and self.weights not in _KNN_WEIGHTS:
                raise ValueError(f"weights must be one of: {sorted(_KNN_WEIGHTS)}")
        if imp == "knn_tn":
            if self.knn_tn_k is not None and not 1 <= self.knn_tn_k <= 10:
                raise ValueError("knn_tn_k must be between 1 and 10")
            if (
                self.knn_tn_distance is not None
                and self.knn_tn_distance not in _KNN_TN_DISTANCE
            ):
                raise ValueError(
                    f"knn_tn_distance must be one of: {sorted(_KNN_TN_DISTANCE)}"
                )
        if imp == "set to constant":
            if self.constant_value is not None and not 0 <= self.constant_value <= 100:
                raise ValueError("constant_value must be between 0 and 100")
        if imp == "mindet":
            if self.q is not None and not 0 <= self.q <= 0.5:
                raise ValueError("q must be between 0 and 0.5")

        # CPM
        if norm == "cpm":
            if self.prior_count is not None and not 0 <= self.prior_count <= 10:
                raise ValueError("prior_count must be between 0 and 10")

        # Filtration per-method param validation
        if filt == "by ptm localization probability":
            if self.threshold is not None and not 0 <= self.threshold <= 1:
                raise ValueError("threshold must be between 0 and 1")
        if filt == "by minimum abundance":
            if (
                self.minimum_abundance_threshold is not None
                and not 0 <= self.minimum_abundance_threshold <= 100
            ):
                raise ValueError(
                    "minimum_abundance_threshold must be between 0 and 100"
                )

        # Shared filter block
        if filt in {"by missing values", "by minimum abundance"}:
            if self.filter_valid_values_criteria is None:
                raise ValueError(
                    f"filter_valid_values_criteria is required for filtration_method="
                    f"'{filt}'"
                )
            if self.filter_valid_values_criteria not in _FILTER_VALID_VALUES_CRITERIA:
                raise ValueError(
                    "filter_valid_values_criteria must be one of: "
                    f"{sorted(_FILTER_VALID_VALUES_CRITERIA)}"
                )
            if self.filter_valid_values_criteria == "percentage":
                if (
                    self.filter_threshold_proportion is not None
                    and not 0 <= self.filter_threshold_proportion <= 1
                ):
                    raise ValueError(
                        "filter_threshold_proportion must be between 0 and 1"
                    )
            elif self.filter_valid_values_criteria == "count":
                if (
                    self.filter_threshold_count is not None
                    and self.filter_threshold_count < 1
                ):
                    raise ValueError("filter_threshold_count must be >= 1")
            if (
                self.filter_valid_values_logic is not None
                and self.filter_valid_values_logic not in _FILTER_VALID_VALUES_LOGIC
            ):
                raise ValueError(
                    "filter_valid_values_logic must be one of: "
                    f"{sorted(_FILTER_VALID_VALUES_LOGIC)}"
                )
            if self.filter_valid_values_logic in {
                "all conditions",
                "at least one condition",
            }:
                if (
                    self.filter_based_on_condition is None
                    or not str(self.filter_based_on_condition).strip()
                ):
                    raise ValueError(
                        "filter_based_on_condition (non-empty str) is required "
                        "for filter_valid_values_logic='"
                        f"{self.filter_valid_values_logic}'"
                    )
            if self.experiment_design is None:
                raise ValueError(
                    "experiment_design is required for filtration_method=" f"'{filt}'"
                )


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

        # entity type — gene is supported via limma (mdFlexiComparisons R/runDiscovery.R).
        # edgeR / DESeq2 (gene-only count engines) are intentionally NOT exposed.
        if self.entity_type not in {"protein", "peptide", "gene"}:
            raise ValueError("entity_type must be one of: protein, peptide, gene")

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
