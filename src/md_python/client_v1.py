"""
V1 API client for the MD Python client
"""

from typing import Optional

from .base_client import BaseMDClient
from .resources import Datasets, Experiments, Health


class MDClientV1(BaseMDClient):
    """V1 API client — experiments, datasets, health"""

    ACCEPT_HEADER = "application/vnd.md-v1+json"

    def __init__(self, api_token: Optional[str] = None, base_url: Optional[str] = None):
        super().__init__(api_token=api_token, base_url=base_url)
        self.health = Health(self)
        self.experiments = Experiments(self)
        self.datasets = Datasets(self)
