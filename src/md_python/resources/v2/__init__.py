"""
V2 resource classes for the MD Python client
"""

from .datasets import Datasets
from .entities import Entities
from .entity_lists import EntityLists
from .jobs import Jobs
from .module_registry import ModuleRegistry
from .uploads import Uploads
from .workspaces import TabModules, Tabs, Workspaces

__all__ = [
    "Uploads",
    "Datasets",
    "Entities",
    "EntityLists",
    "Jobs",
    "Workspaces",
    "Tabs",
    "TabModules",
    "ModuleRegistry",
]
