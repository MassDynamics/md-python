from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ._base import BaseDatasetBuilder

_MOFA_CONVERGENCE_MODES = {"fast", "medium", "slow"}


@pydantic_dataclass
class MOFADataset(BaseDatasetBuilder):
    """Builder for a MOFA+ multi-omics factor analysis dataset.

    MOFA+ integrates two or more INTENSITY datasets ("omics views" — e.g.
    protein abundance and phosphoproteomics) into a set of latent factors.
    All views must share the same sample set; features need not overlap.

    The backend job slug is ``"mofa"`` (the published "MOFA+" job; the
    legacy ``"md_dataset_mofa"`` slug is the older 5-parameter version and
    is intentionally not the default here). Parameter defaults and bounds
    are taken from the live job catalogue (``/jobs`` -> slug "mofa",
    MOFAParams) and md-mofa ``src/md_mofa/process.py``.

    Required: ``input_dataset_ids`` (>= 2 INTENSITY dataset UUIDs),
    ``dataset_name``.

    Optional (all have backend-aligned defaults):
      num_factors           int   2-50,   default 15  (upper bound; MOFA
                                                       auto-prunes below
                                                       drop_factor_threshold)
      convergence_mode      str   fast|medium|slow, default "fast"
      scale_views           bool  default True
      center_groups         bool  default True
      max_iter              int   100-10000, default 1000
      ard_factors           bool  default True   (advanced)
      drop_factor_threshold float 0.0-0.1, default 0.01 (advanced; 0
                                                         disables pruning)
    """

    num_factors: int = 15
    convergence_mode: str = "fast"
    scale_views: bool = True
    center_groups: bool = True
    max_iter: int = 1000
    ard_factors: bool = True
    drop_factor_threshold: float = 0.01
    job_slug: str = "mofa"

    def to_dataset(self) -> Dataset:
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params={
                "num_factors": self.num_factors,
                "convergence_mode": self.convergence_mode,
                "scale_views": self.scale_views,
                "center_groups": self.center_groups,
                "max_iter": self.max_iter,
                "ard_factors": self.ard_factors,
                "drop_factor_threshold": self.drop_factor_threshold,
            },
        )

    @classmethod
    def help(cls) -> str:
        """Return a human-readable description of parameters."""
        lines = [
            "MOFADataset parameters:",
            "- input_dataset_ids (List[str]): >= 2 INTENSITY dataset UUIDs"
            " (the omics views; must share the same samples)",
            "- dataset_name (str): name for the output MOFA dataset",
            "- num_factors (int): 2-50, default 15 (upper bound on factors)",
            "- convergence_mode (str): fast|medium|slow, default 'fast'",
            "- scale_views (bool): scale each view to unit variance," " default True",
            "- center_groups (bool): center features per group, default True",
            "- max_iter (int): 100-10000, default 1000",
            "- ard_factors (bool): ARD sparsity prior on factors, default True",
            "- drop_factor_threshold (float): 0.0-0.1, default 0.01"
            " (min variance per factor; 0 disables auto-pruning)",
            "- job_slug (str): backend job slug, default 'mofa'",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if len(self.input_dataset_ids) < 2:
            raise ValueError(
                "MOFA requires at least 2 input datasets (omics views); "
                f"got {len(self.input_dataset_ids)}"
            )
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not isinstance(self.num_factors, int) or isinstance(self.num_factors, bool):
            raise ValueError("num_factors must be an int")
        if not 2 <= self.num_factors <= 50:
            raise ValueError("num_factors must be between 2 and 50")
        if self.convergence_mode not in _MOFA_CONVERGENCE_MODES:
            raise ValueError(
                "convergence_mode must be one of: " f"{sorted(_MOFA_CONVERGENCE_MODES)}"
            )
        if not isinstance(self.scale_views, bool):
            raise ValueError("scale_views must be a bool")
        if not isinstance(self.center_groups, bool):
            raise ValueError("center_groups must be a bool")
        if not isinstance(self.max_iter, int) or isinstance(self.max_iter, bool):
            raise ValueError("max_iter must be an int")
        if not 100 <= self.max_iter <= 10000:
            raise ValueError("max_iter must be between 100 and 10000")
        if not isinstance(self.ard_factors, bool):
            raise ValueError("ard_factors must be a bool")
        if not 0.0 <= self.drop_factor_threshold <= 0.1:
            raise ValueError("drop_factor_threshold must be between 0.0 and 0.1")
