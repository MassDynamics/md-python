"""Dataset builder classes for the MD Python client.

Each builder constructs a :class:`~md_python.models.dataset.Dataset` for a
particular pipeline (normalisation/imputation, pairwise comparison, ANOVA,
dose-response, MOFA). Split from a single 1,000-line module into one module
per builder; this package re-exports the same public names so the import
path ``md_python.models.dataset_builders`` is unchanged.
"""

from ._base import BaseDatasetBuilder
from .dose_response import DoseResponseDataset
from .minimal import MinimalDataset
from .mofa import MOFADataset
from .normalisation import NormalisationImputationDataset
from .pairwise import PairwiseComparisonDataset

__all__ = [
    "BaseDatasetBuilder",
    "DoseResponseDataset",
    "MinimalDataset",
    "MOFADataset",
    "NormalisationImputationDataset",
    "PairwiseComparisonDataset",
]
