from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ..metadata import SampleMetadata
from ._base import BaseDatasetBuilder
from ._gsea_sets import GSEA_DEFAULT_SETS, GSEA_SETS_BY_SPECIES, GseaSetsValidator

# Knowledge bases are species-conditional; the validator owns the catalogue and
# the fail-fast checking (see _gsea_sets.py — the backend silently drops
# unrecognised sets, so an unknown value must never reach the wire).
_SETS_VALIDATOR = GseaSetsValidator()

# Entity types accepted by the enrichment params model
# (EnrichmentParamsProperties.entity_type enum). The catalogue notes peptide
# is "not supported" in the UI description despite being in the enum.
_GSEA_ENTITY_TYPES = {"gene", "protein"}

# Organisms recognised by the CAMERA GSEA databases
# (EnrichmentParamsProperties.species enum). Title-cased on the wire.
_GSEA_SPECIES = {"Human", "Mouse", "Chinese hamster", "Yeast"}

_GSEA_FILTER_LOGIC = {
    "all conditions",
    "at least one condition",
    "full experiment",
}


@pydantic_dataclass
class GseaDataset(BaseDatasetBuilder):
    """Builder for a CAMERA GSEA (gene-set enrichment) dataset.

    CAMERA (Wu & Smyth, 2012) is a competitive gene-set test that accounts
    for inter-gene correlation. It tests whether genes in a set are
    differentially expressed relative to genes outside the set, for each
    pairwise comparison derived from ``condition_column`` and
    ``condition_comparisons``.

    The backend job slug is ``"camera_gsea"`` and the output dataset type is
    ``"ENRICHMENT"``. Parameter names, enums, defaults and required-ness are
    taken verbatim from the live job catalogue (``/jobs`` -> slug
    "camera_gsea", ``EnrichmentParamsProperties``).

    Required: ``input_dataset_ids`` (exactly one INTENSITY dataset UUID),
    ``dataset_name``, ``sample_metadata``, ``condition_column``,
    ``condition_comparisons``, ``species``.

    Optional (all have backend-aligned defaults):
      entity_type        str   gene|protein, default "protein"
      sets               list[str]  knowledge bases, SPECIES-CONDITIONAL enum —
                               see GSEA_SETS_BY_SPECIES in _gsea_sets.py. Default
                               is the three GO sets. An unrecognised value raises
                               ValueError (the backend would silently drop it and
                               still report COMPLETED).
      filter_values_criteria dict  default
                               {"method": "percentage",
                                "filter_threshold_percentage": 0.5}
      filter_valid_values_logic str  default "at least one condition"
      limma_trend        bool  default True
      robust_empirical_bayes bool  default True
      fit_separate_models bool  default True
      control_variables  dict  default None
    """

    # input_dataset_ids, dataset_name inherited.
    sample_metadata: SampleMetadata
    condition_column: str
    condition_comparisons: List[List[str]]
    species: str
    entity_type: str = "protein"
    sets: Optional[List[str]] = None
    filter_values_criteria: Optional[Dict[str, Any]] = None
    filter_valid_values_logic: str = "at least one condition"
    limma_trend: bool = True
    robust_empirical_bayes: bool = True
    fit_separate_models: bool = True
    control_variables: Optional[Dict[str, List[Dict[str, str]]]] = None
    job_slug: str = "camera_gsea"

    def __post_init__(self) -> None:
        # Mutable defaults — backend defaults from EnrichmentParamsProperties.
        if self.sets is None:
            self.sets = list(GSEA_DEFAULT_SETS)
        if self.filter_values_criteria is None:
            self.filter_values_criteria = {
                "method": "percentage",
                "filter_threshold_percentage": 0.5,
            }

    def to_dataset(self) -> Dataset:
        # NOTE: the output dataset type ("ENRICHMENT") is NOT a member of
        # job_run_params. The create path POSTs a flat payload and the server
        # derives the output type from the job slug's run_type. Embedding it
        # here would ship an unexpected key into EnrichmentParamsProperties,
        # which has a fixed field set — mirror MOFADataset and send params only.
        params: Dict[str, Any] = {
            "entity_type": self.entity_type,
            "species": self.species,
            # Canonicalised (and hard-validated) — an unrecognised value is a
            # ValueError here, never a silent drop on the backend.
            "sets": _SETS_VALIDATOR.canonicalise(self.sets or [], self.species),
            "condition_column": self.condition_column,
            "condition_comparisons": {
                "condition_comparison_pairs": self.condition_comparisons
            },
            "experiment_design": self.sample_metadata.to_columns(),
            "filter_values_criteria": self.filter_values_criteria,
            "filter_valid_values_logic": self.filter_valid_values_logic,
            "limma_trend": self.limma_trend,
            "robust_empirical_bayes": self.robust_empirical_bayes,
            "fit_separate_models": self.fit_separate_models,
            "control_variables": self.control_variables,
        }
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params=params,
        )

    @classmethod
    def help(cls) -> str:
        """Return a human-readable description of parameters."""
        lines = [
            "GseaDataset parameters:",
            "- input_dataset_ids (List[str]): exactly one INTENSITY dataset UUID",
            "- dataset_name (str): name for the output ENRICHMENT dataset",
            "- sample_metadata (SampleMetadata): metadata table used for grouping",
            "- condition_column (str): column in sample_metadata defining groups",
            "- condition_comparisons (List[List[str]]): list of [case, control] pairs",
            "- species (str): Human|Mouse|Chinese hamster|Yeast",
            "- entity_type (str): gene|protein, default 'protein'",
            "- sets (List[str]): knowledge bases, default the three GO sets."
            " Valid values depend on species:",
            *(
                f"    {species}: {values}"
                for species, values in GSEA_SETS_BY_SPECIES.items()
            ),
            "- filter_values_criteria (dict): {method: percentage|count, ...},"
            " default percentage 0.5",
            "- filter_valid_values_logic (str): one of {all conditions,"
            " at least one condition, full experiment}, default 'at least one condition'",
            "- limma_trend (bool): default True",
            "- robust_empirical_bayes (bool): default True",
            "- fit_separate_models (bool): default True",
            "- control_variables (dict): optional covariates",
            "- job_slug (str): backend job slug, default 'camera_gsea'",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if len(self.input_dataset_ids) != 1:
            raise ValueError(
                "GSEA requires exactly 1 input dataset (the INTENSITY dataset); "
                f"got {len(self.input_dataset_ids)}"
            )
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
        if self.entity_type not in _GSEA_ENTITY_TYPES:
            raise ValueError(
                f"entity_type must be one of: {sorted(_GSEA_ENTITY_TYPES)}"
            )
        if self.species not in _GSEA_SPECIES:
            raise ValueError(f"species must be one of: {sorted(_GSEA_SPECIES)}")
        # Species-conditional enum. An unknown value is REJECTED, not dropped —
        # the backend accepts it, reports COMPLETED, and never runs it.
        _SETS_VALIDATOR.canonicalise(self.sets or [], self.species)
        if self.filter_valid_values_logic not in _GSEA_FILTER_LOGIC:
            raise ValueError(
                "filter_valid_values_logic must be one of: "
                f"{sorted(_GSEA_FILTER_LOGIC)}"
            )
        if not isinstance(self.limma_trend, bool):
            raise ValueError("limma_trend must be a bool")
        if not isinstance(self.robust_empirical_bayes, bool):
            raise ValueError("robust_empirical_bayes must be a bool")
        if not isinstance(self.fit_separate_models, bool):
            raise ValueError("fit_separate_models must be a bool")

        crit = self.filter_values_criteria
        if crit is None or not hasattr(crit, "get"):
            raise ValueError("filter_values_criteria must be a dict-like object")
        method = crit.get("method")
        if method not in ["percentage", "count"]:
            raise ValueError(
                "filter_values_criteria method must be one of: percentage, count"
            )
        if method == "percentage":
            pct = crit.get("filter_threshold_percentage")
            if pct is not None and (pct < 0 or pct > 1):
                raise ValueError(
                    "filter_values_criteria filter_threshold_percentage "
                    "must be between 0 and 1"
                )
        elif method == "count":
            cnt = crit.get("filter_threshold_count")
            if cnt is not None and cnt < 1:
                raise ValueError(
                    "filter_values_criteria filter_threshold_count must be >= 1"
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
            for item in items:
                if not isinstance(item, dict):
                    raise ValueError("each control variable must be a dictionary")
                if "column" not in item or "type" not in item:
                    raise ValueError(
                        "each control variable must include 'column' and 'type'"
                    )
                if item.get("type") not in {"numerical", "categorical"}:
                    raise ValueError(
                        "control variable 'type' must be one of: numerical, categorical"
                    )
