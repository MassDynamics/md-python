from typing import Any, Dict, List, Optional, Sequence
from uuid import UUID

from pydantic import field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ._base import BaseDatasetBuilder
from ._methods import (
    _BATCH_CORRECTION_TECHNIQUES_GENE,
    _BATCH_CORRECTION_TECHNIQUES_PROTEOMICS,
    _ENTITY_TYPES,
    _FILTER_VALID_VALUES_CRITERIA,
    _FILTER_VALID_VALUES_LOGIC,
    _GENE_FILTRATION_METHODS,
    _GENE_NORMALISATION_METHODS,
    _IMPUTATION_METHODS,
    _KNN_TN_DISTANCE,
    _KNN_WEIGHTS,
    _METABOLITE_FILTRATION_METHODS,
    _PEPTIDE_FILTRATION_METHODS,
    _PROTEIN_FILTRATION_METHODS,
    _PROTEOMICS_NORMALISATION_METHODS,
    _PTM_FILTRATION_METHODS,
    _batch_correction_technique_key,
    _canon_method,
    _filtration_methods_key,
    _normalisation_methods_key,
)
from ._normalisation_help import NORMALISATION_HELP_TEXT

# The wire format for experiment_design is a column-oriented dict
# (SampleMetadata.to_columns()). Callers routinely hold the ROW-oriented form
# instead — it is what get_dataset, get_upload_sample_metadata and
# load_metadata_from_csv all emit, and what run_gsea / run_anova /
# run_pairwise_comparison take as sample_metadata. Both shapes are accepted on
# input and coerced to the column dict; anything else raises a ValueError that
# names the two accepted shapes.
_EXPERIMENT_DESIGN_SHAPES = (
    "experiment_design accepts exactly two shapes: (1) a column-oriented dict, "
    "e.g. {'sample_name': ['s1', 's2'], 'condition': ['a', 'b']} "
    "(SampleMetadata.to_columns()); or (2) a row-oriented list of lists whose "
    "first row is the header, e.g. [['sample_name', 'condition'], "
    "['s1', 'a'], ['s2', 'b']] (exactly what get_dataset / "
    "load_metadata_from_csv return)."
)


def _rows_to_columns(rows: Sequence[Any]) -> Dict[str, List[str]]:
    """Coerce a row-oriented experiment_design (header row + data rows) to columns.

    Raises ValueError naming both accepted shapes when *rows* is empty, ragged,
    header-only, has duplicate header names, or is not a list of lists.
    """
    if not rows:
        raise ValueError(
            f"experiment_design cannot be empty. {_EXPERIMENT_DESIGN_SHAPES}"
        )
    if any(not isinstance(row, (list, tuple)) for row in rows):
        raise ValueError(
            "experiment_design was given as a list, but its entries are not rows "
            f"(lists) — got {[type(r).__name__ for r in rows][:3]}. "
            f"{_EXPERIMENT_DESIGN_SHAPES}"
        )

    header = list(rows[0])
    if not header or any(not isinstance(h, str) or not h.strip() for h in header):
        raise ValueError(
            "experiment_design row-oriented form requires a non-empty header row "
            f"of column-name strings — got {rows[0]!r} as the first row. "
            f"{_EXPERIMENT_DESIGN_SHAPES}"
        )
    headers = [h.strip() for h in header]
    if len(set(headers)) != len(headers):
        raise ValueError(
            f"experiment_design header row has duplicate column names: {headers}. "
            f"{_EXPERIMENT_DESIGN_SHAPES}"
        )
    if len(rows) == 1:
        raise ValueError(
            "experiment_design row-oriented form has a header row but no sample "
            f"rows: {headers}. {_EXPERIMENT_DESIGN_SHAPES}"
        )

    columns: Dict[str, List[str]] = {h: [] for h in headers}
    for i, row in enumerate(rows[1:], start=1):
        if len(row) != len(headers):
            raise ValueError(
                f"experiment_design row {i} has {len(row)} values but the header "
                f"has {len(headers)} columns ({headers}) — rows must not be "
                f"ragged. Offending row: {list(row)!r}. {_EXPERIMENT_DESIGN_SHAPES}"
            )
        for h, value in zip(headers, row):
            columns[h].append(str(value))
    return columns


@pydantic_dataclass
class NormalisationImputationDataset(BaseDatasetBuilder):
    """Builder for the normalisation + imputation + filtration pipeline.

    The job runs three steps in fixed order: filtration -> normalisation -> imputation.
    Each step can be skipped independently. The output dataset is of type INTENSITY.

    Required: ``input_dataset_ids``, ``dataset_name``, ``normalisation_method``,
    ``imputation_method``. ``entity_type`` defaults to ``"protein"``.

    ``experiment_design`` accepts BOTH the column-oriented dict wire format
    (``SampleMetadata.to_columns()``) and the row-oriented list-of-lists form
    (header row + data rows) that ``get_dataset`` / ``load_metadata_from_csv``
    emit; the row form is coerced to columns on construction.

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

    @field_validator("experiment_design", mode="before")
    @classmethod
    def _accept_row_or_column_experiment_design(cls, value: Any) -> Any:
        """Accept the row-oriented list-of-lists form as well as the column dict.

        The MCP's own ``get_dataset`` hands the model a row-oriented
        ``experiment_design``; passing it straight back in used to raise a raw
        pydantic ``dict_type`` error. Rows are coerced here; dicts pass through.
        """
        if value is None or isinstance(value, dict):
            return value
        if isinstance(value, (list, tuple)):
            return _rows_to_columns(value)
        raise ValueError(
            f"experiment_design has unsupported type '{type(value).__name__}'. "
            f"{_EXPERIMENT_DESIGN_SHAPES}"
        )

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
    def help(cls) -> str:
        """Return a human-readable description of supported methods and params."""
        return NORMALISATION_HELP_TEXT

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
            "ptm": _PTM_FILTRATION_METHODS,
            "metabolite": _METABOLITE_FILTRATION_METHODS,
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
                    "batch_correction_technique is required because "
                    "normalisation_method='batch correction'. Pass one of "
                    f"{sorted(_BATCH_CORRECTION_TECHNIQUES_GENE if self.entity_type == 'gene' else _BATCH_CORRECTION_TECHNIQUES_PROTEOMICS)} "
                    "via normalisation_extra_params."
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
                        "batch_variables (non-empty list) is required because "
                        "batch_correction_technique='limma remove batch effect'. "
                        "Pass one entry per batch column via "
                        "normalisation_extra_params, e.g. "
                        "batch_variables=[{'column': 'batch', "
                        "'type': 'categorical'}]."
                    )
            elif technique in {"combat", "combat seq"}:
                if (
                    self.batch_variable_combat is None
                    or not str(self.batch_variable_combat).strip()
                ):
                    raise ValueError(
                        "batch_variable_combat (non-empty str) is required because "
                        f"batch_correction_technique='{technique}'. Pass the single "
                        "batch column name from experiment_design via "
                        "normalisation_extra_params, e.g. "
                        "batch_variable_combat='batch'."
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
                    "experiment_design is required because normalisation_method="
                    "'batch correction'. Pass it via normalisation_extra_params. "
                    f"{_EXPERIMENT_DESIGN_SHAPES}"
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
                    "filter_valid_values_criteria is required because "
                    f"filtration_method='{filt}'. Pass 'percentage' (with "
                    "filter_threshold_proportion, 0.0-1.0) or 'count' (with "
                    "filter_threshold_count, >=1) via filtration_extra_params."
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
                        "because filter_valid_values_logic='"
                        f"{self.filter_valid_values_logic}'. Pass the name of the "
                        "condition column in experiment_design (e.g. 'condition') "
                        "via filtration_extra_params, or set "
                        "filter_valid_values_logic='full experiment' to apply the "
                        "threshold across the whole experiment instead."
                    )
            if self.experiment_design is None:
                raise ValueError(
                    "experiment_design is required because filtration_method="
                    f"'{filt}'. Pass it via filtration_extra_params. "
                    f"{_EXPERIMENT_DESIGN_SHAPES}"
                )
