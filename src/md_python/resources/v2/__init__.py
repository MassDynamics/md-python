"""
V2 resource classes for the MD Python client
"""

from .datasets import Datasets
from .entities import Entities
from .entity_lists import EntityLists
from .evosep_qcs import EvosepQcs
from .jobs import Jobs
from .module_registry import ModuleRegistry
from .uploads import Uploads
from .workspaces import TabModules, Tabs, Workspaces

__all__ = [
    "Uploads",
    "Datasets",
    "Entities",
    "EntityLists",
    "EvosepQcs",
    "Jobs",
    "Workspaces",
    "Tabs",
    "TabModules",
    "ModuleRegistry",
]
