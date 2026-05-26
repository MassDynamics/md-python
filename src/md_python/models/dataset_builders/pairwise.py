from dataclasses import field
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ..metadata import SampleMetadata
from ._base import BaseDatasetBuilder
from ._methods import _DE_METHODS_PER_ENTITY, _ENTITY_TYPES, _de_method_key


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
        One of: "protein", "peptide", "gene", "metabolite", "ptm". Default:
        "protein". Backend accepts lowercase only — the UI shows "PTM" and
        "Metabolite" but the wire is lowercase. Gene / metabolite / ptm
        pairwise runs through limma (mdFlexiComparisons runDiscovery,
        de_method='limma'). edgeR / DESeq2 are NOT exposed by this MCP.
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
    # DE engine for the differential test. Only gene exposes a real choice
    # (limma | edgeR | DESeq2); every other entity_type is limma-only per the
    # MDFlexiComparisons schema. Emitted on the wire as ``de_method_<entity_type>``.
    de_method: str = "limma"
    # edgeR / DESeq2 companion params — sent only when de_method warrants it.
    edger_norm_method: str = "TMM"
    deseq2_lfc_shrinkage: str = "none"
    deseq2_alpha: float = 0.05
    apeglm_seed: int = 1
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
        params: Dict[str, Any] = {
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
            # DE method on the wire is entity-keyed: emit ``de_method_<entity>``
            # so the MDFlexiComparisons Pydantic schema picks it up. The other
            # four per-entity de_method fields default to limma in the schema.
            _de_method_key(self.entity_type): self.de_method,
        }
        # edgeR / DESeq2 companion params — only emit when the chosen DE
        # method needs them; otherwise let the backend defaults stand.
        if self.de_method == "edgeR":
            params["edger_norm_method"] = self.edger_norm_method
        elif self.de_method == "DESeq2":
            params["deseq2_lfc_shrinkage"] = self.deseq2_lfc_shrinkage
            params["deseq2_alpha"] = self.deseq2_alpha
            if self.deseq2_lfc_shrinkage == "apeglm":
                params["apeglm_seed"] = self.apeglm_seed
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params=params,
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
            "- entity_type (str): protein|peptide|gene|metabolite|ptm (default protein)",
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

        # Entity types are sourced from _ENTITY_TYPES so this validator stays in
        # lock-step with the rest of the package. As of 2026-05-27 the live
        # backend stores pairwise jobs with entity_type in
        # {protein, peptide, gene, metabolite, ptm} — see
        # project_pairwise_de_method_entity_keyed memory.
        if self.entity_type not in _ENTITY_TYPES:
            raise ValueError(
                f"entity_type must be one of: {sorted(_ENTITY_TYPES)}"
            )

        # DE method gating. MDFlexiComparisons Pydantic schema only allows
        # edgeR / DESeq2 for entity_type='gene'; sending those for any other
        # entity will be rejected by the per-entity ``Literal["limma"]``.
        allowed_de = _DE_METHODS_PER_ENTITY[self.entity_type]
        if self.de_method not in allowed_de:
            raise ValueError(
                f"de_method '{self.de_method}' not allowed for "
                f"entity_type='{self.entity_type}'. "
                f"Allowed: {sorted(allowed_de)}"
            )

        # edgeR companion: validate norm method enum.
        if self.de_method == "edgeR":
            if self.edger_norm_method not in {
                "TMM",
                "RLE",
                "upperquartile",
                "none",
            }:
                raise ValueError(
                    "edger_norm_method must be one of: "
                    "TMM, RLE, upperquartile, none "
                    f"(got '{self.edger_norm_method}')"
                )

        # DESeq2 companions: validate shrinkage enum + alpha range + seed range.
        if self.de_method == "DESeq2":
            if self.deseq2_lfc_shrinkage not in {
                "none",
                "apeglm",
                "ashr",
                "normal",
            }:
                raise ValueError(
                    "deseq2_lfc_shrinkage must be one of: "
                    "none, apeglm, ashr, normal "
                    f"(got '{self.deseq2_lfc_shrinkage}')"
                )
            if not 0.0 <= self.deseq2_alpha <= 1.0:
                raise ValueError(
                    "deseq2_alpha must be between 0 and 1 "
                    f"(got {self.deseq2_alpha})"
                )
            if self.deseq2_lfc_shrinkage == "apeglm":
                if not 0 <= self.apeglm_seed <= 2147483647:
                    raise ValueError(
                        "apeglm_seed must be between 0 and 2147483647 "
                        f"(got {self.apeglm_seed})"
                    )

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
