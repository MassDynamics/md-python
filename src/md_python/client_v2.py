"""
V2 API client for the MD Python client
"""

from typing import Optional

from .base_client import BaseMDClient
from .resources import Health
from .resources.v2 import Datasets, Entities, Jobs, Uploads


class MDClientV2(BaseMDClient):
    """V2 API client — uploads, datasets, jobs, entities, health"""

    ACCEPT_HEADER = "application/vnd.md-v2+json"

    def __init__(self, api_token: Optional[str] = None, base_url: Optional[str] = None):
        super().__init__(api_token=api_token, base_url=base_url)
        self.health = Health(self)
        self.uploads = Uploads(self)
        self.datasets = Datasets(self)
        self.jobs = Jobs(self)
        self.entities = Entities(self)
