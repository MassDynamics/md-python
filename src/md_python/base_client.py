"""
Base client class for the MD Python client
"""

import os
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

DEFAULT_BASE_URL = "https://app.massdynamics.com/api"


class BaseMDClient:
    """Base client with shared auth, base URL, and HTTP transport"""

    ACCEPT_HEADER: str

    base_url: str
    api_token: str

    def __init__(self, api_token: Optional[str] = None, base_url: Optional[str] = None):
        base = base_url or os.getenv("MD_API_BASE_URL") or DEFAULT_BASE_URL
        token = api_token or os.getenv("MD_AUTH_TOKEN")

        if not token:
            raise ValueError("MD_AUTH_TOKEN must be set or passed as api_token")

        self.base_url: str = base
        self.api_token: str = token

    def _get_headers(self) -> dict:
        """Get common headers for API requests"""
        return {
            "accept": self.ACCEPT_HEADER,
            "Authorization": f"Bearer {self.api_token}",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Make HTTP request to the API"""
        url = f"{self.base_url}{endpoint}"
        request_headers = self._get_headers()

        if headers:
            request_headers.update(headers)

        return requests.request(
            method, url, headers=request_headers, json=json, **kwargs
        )
