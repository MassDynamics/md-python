"""
MD Python Client - A Python client for the Mass Dynamics API
"""

from .base_client import BaseMDClient
from .client import MDClient
from .client_v1 import MDClientV1
from .client_v2 import MDClientV2
from .models import (
    Dataset,
    DoseResponseDataset,
    Experiment,
    ExperimentDesign,
    MinimalDataset,
    NormalisationImputationDataset,
    PairwiseComparisonDataset,
    SampleMetadata,
    Upload,
)
from .resources import Datasets, Experiments, Health

__all__ = [
    "MDClient",
    "MDClientV1",
    "MDClientV2",
    "BaseMDClient",
    "Experiment",
    "Upload",
    "Dataset",
    "SampleMetadata",
    "ExperimentDesign",
    "Health",
    "Experiments",
    "Datasets",
    "PairwiseComparisonDataset",
    "DoseResponseDataset",
    "MinimalDataset",
    "NormalisationImputationDataset",
]

# ---------------------------------------------------------------------------
# Fixture recording / replay — eval testing only.
# Activates when MD_RECORD_FIXTURES or MD_REPLAY_FIXTURES env var is set.
# Completely inert otherwise (no performance impact in production).
# ---------------------------------------------------------------------------
import os as _os

if _os.environ.get("MD_RECORD_FIXTURES") or _os.environ.get("MD_REPLAY_FIXTURES"):
    from . import fixtures as _fixtures
    _fixtures.install()
