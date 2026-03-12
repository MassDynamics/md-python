"""
Jobs resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Jobs:
    """V2 jobs resource"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def list(self) -> List[Dict[str, Any]]:
        """List all available dataset jobs.

        Returns:
            List of job dictionaries with id, name, slug, etc.
        """
        response = self._client._make_request(
            method="GET",
            endpoint="/jobs",
        )

        if response.status_code == 200:
            result: List[Dict[str, Any]] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to list jobs: {response.status_code} - {response.text}"
            )
