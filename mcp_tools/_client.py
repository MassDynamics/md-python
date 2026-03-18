import os
from typing import Any

from md_python.client import MDClient


def get_client() -> Any:
    return MDClient(
        api_token=os.environ.get("MD_AUTH_TOKEN"),
        base_url=os.environ.get("MD_API_BASE_URL"),
    )
