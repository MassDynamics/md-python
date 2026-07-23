"""
V2 API client for the MD Python client
"""

from typing import Optional

from .base_client import BaseMDClient
from .resources import Health
from .resources.v2 import Datasets, Entities, Jobs, ModuleRegistry, Uploads, Workspaces


class MDClientV2(BaseMDClient):
    """V2 API client — uploads, datasets, entities, jobs, workspaces, health"""

    ACCEPT_HEADER = "application/vnd.md-v2+json"

    def __init__(self, api_token: Optional[str] = None, base_url: Optional[str] = None):
        super().__init__(api_token=api_token, base_url=base_url)
        self.health = Health(self)
        self.uploads = Uploads(self)
        self.datasets = Datasets(self)
        self.entities = Entities(self)
        self.jobs = Jobs(self)
        self.module_registry = ModuleRegistry(self)
        # Pass the same module_registry instance into Workspaces so
        # create_with_defaults() reuses it instead of spinning up a duplicate.
        self.workspaces = Workspaces(self, registry=self.module_registry)
