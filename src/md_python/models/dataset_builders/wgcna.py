from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ..metadata import SampleMetadata
from ._base import BaseDatasetBuilder

# Entity types accepted by the WGCNA params model (WGCNAParams.entity_type enum).
_WGCNA_ENTITY_TYPES = {"protein", "peptide", "gene"}

# Network types (WGCNAParams.network_type enum).
_WGCNA_NETWORK_TYPES = {"unsigned", "signed", "signed hybrid"}

# filter_method options (WGCNAParams.filter_method) — None skips the filter,
# "goodSamplesGenes" enables the iterative good-samples/genes filter.
_WGCNA_FILTER_METHODS = {None, "goodSamplesGenes"}


@pydantic_dataclass
class WgcnaDataset(BaseDatasetBuilder):
    """Builder for a WGCNA co-expression network dataset.

    WGCNA (PyWGCNA, Rezaie et al. 2023) builds a weighted correlation
    network over entities, detects co-expression modules, summarises each
    module with an eigenentity, and correlates module eigenentities with
    sample-metadata trait columns.

    The backend job slug is ``"wgcna"`` and the output dataset type is
    ``"WGCNA"``. Parameter names, types, bounds, defaults and the
    ``goodSamplesGenes`` conditional-visibility clause are taken verbatim
    from the live job catalogue (``/jobs`` -> slug "wgcna", ``WGCNAParams``).

    Required: ``input_dataset_ids`` (exactly one INTENSITY dataset UUID),
    ``dataset_name``.

    Optional (all have backend-aligned defaults):
      entity_type        str   protein|peptide|gene, default "protein"
      sample_metadata    SampleMetadata  trait table; default None
      trait_columns      list[str]  metadata columns to correlate; default None
      log_transform      bool  default True
      network_type       str   unsigned|signed|signed hybrid, default "signed"
      min_module_size    int   >= 2, default 30
      merge_cut_height   float 0.0-1.0, default 0.25
      soft_power         int   1-30 or None (auto-select), default None
      rsquared_cut       float 0.0-1.0, default 0.9
      mean_connectivity_cut int >= 1, default 100
      deep_split         int   0-4, default 2
      filter_method      str   None | "goodSamplesGenes", default None
      # goodSamplesGenes sub-params (only sent when filter_method set):
      min_fraction       float 0.0-1.0, default 0.5
      min_n_samples      int   >= 1, default 4
      min_n_genes        int   >= 1, default 4
      min_relative_weight float 0.0-1.0, default 0.1
      tol                float >= 0.0 or None (auto), default None
    """

    # input_dataset_ids, dataset_name inherited.
    sample_metadata: Optional[SampleMetadata] = None
    trait_columns: Optional[List[str]] = None
    entity_type: str = "protein"
    log_transform: bool = True
    network_type: str = "signed"
    min_module_size: int = 30
    merge_cut_height: float = 0.25
    soft_power: Optional[int] = None
    rsquared_cut: float = 0.9
    mean_connectivity_cut: int = 100
    deep_split: int = 2
    filter_method: Optional[str] = None
    # goodSamplesGenes sub-parameters (only emitted when filter_method is set).
    min_fraction: float = 0.5
    min_n_samples: int = 4
    min_n_genes: int = 4
    min_relative_weight: float = 0.1
    tol: Optional[float] = None
    job_slug: str = "wgcna"

    def to_dataset(self) -> Dataset:
        # NOTE: the output dataset type ("WGCNA") is NOT a member of
        # job_run_params. The create path POSTs a flat payload and the server
        # derives the output type from the job slug's run_type. Embedding it
        # here would ship an unexpected key into WGCNAParams, which has a fixed
        # field set — mirror MOFADataset and send params only.
        params: Dict[str, Any] = {
            "entity_type": self.entity_type,
            "trait_columns": self.trait_columns,
            "log_transform": self.log_transform,
            "network_type": self.network_type,
            "min_module_size": self.min_module_size,
            "merge_cut_height": self.merge_cut_height,
            "soft_power": self.soft_power,
            "rsquared_cut": self.rsquared_cut,
            "mean_connectivity_cut": self.mean_connectivity_cut,
            "deep_split": self.deep_split,
            "filter_method": self.filter_method,
        }
        if self.sample_metadata is not None:
            params["experiment_design"] = self.sample_metadata.to_columns()
        # The goodSamplesGenes sub-params are only visible / honoured when the
        # filter is enabled (WGCNAParams `when filter_method == goodSamplesGenes`).
        if self.filter_method == "goodSamplesGenes":
            params["min_fraction"] = self.min_fraction
            params["min_n_samples"] = self.min_n_samples
            params["min_n_genes"] = self.min_n_genes
            params["min_relative_weight"] = self.min_relative_weight
            params["tol"] = self.tol
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
            "WgcnaDataset parameters:",
            "- input_dataset_ids (List[str]): exactly one INTENSITY dataset UUID",
            "- dataset_name (str): name for the output WGCNA dataset",
            "- sample_metadata (SampleMetadata): trait table (optional)",
            "- trait_columns (List[str]): metadata columns to correlate (optional)",
            "- entity_type (str): protein|peptide|gene, default 'protein'",
            "- log_transform (bool): default True",
            "- network_type (str): unsigned|signed|signed hybrid, default 'signed'",
            "- min_module_size (int): >= 2, default 30",
            "- merge_cut_height (float): 0.0-1.0, default 0.25",
            "- soft_power (int): 1-30 or None (auto-select), default None",
            "- rsquared_cut (float): 0.0-1.0, default 0.9",
            "- mean_connectivity_cut (int): >= 1, default 100",
            "- deep_split (int): 0-4, default 2",
            "- filter_method (str): None | 'goodSamplesGenes', default None",
            "- min_fraction (float): 0.0-1.0, default 0.5 (goodSamplesGenes only)",
            "- min_n_samples (int): >= 1, default 4 (goodSamplesGenes only)",
            "- min_n_genes (int): >= 1, default 4 (goodSamplesGenes only)",
            "- min_relative_weight (float): 0.0-1.0, default 0.1"
            " (goodSamplesGenes only)",
            "- tol (float): >= 0.0 or None (auto), default None"
            " (goodSamplesGenes only)",
            "- job_slug (str): backend job slug, default 'wgcna'",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if len(self.input_dataset_ids) != 1:
            raise ValueError(
                "WGCNA requires exactly 1 input dataset (the INTENSITY dataset); "
                f"got {len(self.input_dataset_ids)}"
            )
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if self.entity_type not in _WGCNA_ENTITY_TYPES:
            raise ValueError(
                f"entity_type must be one of: {sorted(_WGCNA_ENTITY_TYPES)}"
            )
        if self.network_type not in _WGCNA_NETWORK_TYPES:
            raise ValueError(
                f"network_type must be one of: {sorted(_WGCNA_NETWORK_TYPES)}"
            )
        if not isinstance(self.log_transform, bool):
            raise ValueError("log_transform must be a bool")
        if not isinstance(self.min_module_size, int) or isinstance(
            self.min_module_size, bool
        ):
            raise ValueError("min_module_size must be an int")
        if self.min_module_size < 2:
            raise ValueError("min_module_size must be >= 2")
        if not 0.0 <= self.merge_cut_height <= 1.0:
            raise ValueError("merge_cut_height must be between 0.0 and 1.0")
        if self.soft_power is not None:
            if not isinstance(self.soft_power, int) or isinstance(
                self.soft_power, bool
            ):
                raise ValueError("soft_power must be an int or None")
            if not 1 <= self.soft_power <= 30:
                raise ValueError("soft_power must be between 1 and 30")
        if not 0.0 <= self.rsquared_cut <= 1.0:
            raise ValueError("rsquared_cut must be between 0.0 and 1.0")
        if not isinstance(self.mean_connectivity_cut, int) or isinstance(
            self.mean_connectivity_cut, bool
        ):
            raise ValueError("mean_connectivity_cut must be an int")
        if self.mean_connectivity_cut < 1:
            raise ValueError("mean_connectivity_cut must be >= 1")
        if not isinstance(self.deep_split, int) or isinstance(self.deep_split, bool):
            raise ValueError("deep_split must be an int")
        if not 0 <= self.deep_split <= 4:
            raise ValueError("deep_split must be between 0 and 4")
        if self.filter_method not in _WGCNA_FILTER_METHODS:
            raise ValueError("filter_method must be one of: None, 'goodSamplesGenes'")
        if self.filter_method == "goodSamplesGenes":
            if not 0.0 <= self.min_fraction <= 1.0:
                raise ValueError("min_fraction must be between 0.0 and 1.0")
            if not isinstance(self.min_n_samples, int) or isinstance(
                self.min_n_samples, bool
            ):
                raise ValueError("min_n_samples must be an int")
            if self.min_n_samples < 1:
                raise ValueError("min_n_samples must be >= 1")
            if not isinstance(self.min_n_genes, int) or isinstance(
                self.min_n_genes, bool
            ):
                raise ValueError("min_n_genes must be an int")
            if self.min_n_genes < 1:
                raise ValueError("min_n_genes must be >= 1")
            if not 0.0 <= self.min_relative_weight <= 1.0:
                raise ValueError("min_relative_weight must be between 0.0 and 1.0")
            if self.tol is not None and self.tol < 0.0:
                raise ValueError("tol must be >= 0.0 or None")
