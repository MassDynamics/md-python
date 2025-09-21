"""
Main client class for the MD Python client
"""

import os
from typing import Optional

import requests
from dotenv import load_dotenv

from .resources import Datasets, Experiments, Health

# Load environment variables from .env file
load_dotenv()


class MDClient:
    """Enhanced MD Client that combines simplicity with type safety"""

    base_url: str  # Default base URL
    api_token: str

    def __init__(self, api_token: Optional[str] = None, base_url: Optional[str] = None):
        
        self.base_url = base_url or os.getenv("MD_API_BASE_URL")

        self.api_token = api_token or os.getenv("MD_AUTH_TOKEN")

        # Nested resource structure
        self.health = Health(self)
        self.experiments = Experiments(self)
        self.datasets = Datasets(self)

    def _get_headers(self) -> dict:
        """Get common headers for API requests"""
        return {
            "accept": "application/vnd.md-v1+json",
            "Authorization": f"Bearer {self.api_token}",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """Make HTTP request to the API"""
        url = f"{self.base_url}{endpoint}"
        request_headers = self._get_headers()

        # Merge any additional headers if provided
        if headers:
            request_headers.update(headers)

        return requests.request(method, url, headers=request_headers, json=json)
