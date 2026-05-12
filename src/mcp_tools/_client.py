import os

from md_python.client_v2 import MDClientV2


def get_client() -> MDClientV2:
    return MDClientV2(
        api_token=os.environ.get("MD_AUTH_TOKEN"),
        base_url=os.environ.get("MD_API_BASE_URL"),
    )
