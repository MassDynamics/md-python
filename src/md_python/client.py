"""
MDClient factory for the MD Python client
"""

from typing import Optional

from .base_client import BaseMDClient
from .client_v1 import MDClientV1
from .client_v2 import MDClientV2


def MDClient(
    api_token: Optional[str] = None,
    base_url: Optional[str] = None,
    version: str = "v1",
) -> BaseMDClient:
    """Factory that returns the correct client for the requested API version.

    Args:
        api_token: Bearer token for authentication
        base_url: API base URL (defaults to MD_API_BASE_URL env var or production)
        version: API version — "v1" or "v2"

    Returns:
        MDClientV1 or MDClientV2
    """
    if version == "v1":
        return MDClientV1(api_token=api_token, base_url=base_url)
    if version == "v2":
        return MDClientV2(api_token=api_token, base_url=base_url)
    raise ValueError(f"Unsupported API version: {version}. Use 'v1' or 'v2'.")
