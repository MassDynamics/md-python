"""
Jobs resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict, List

from ...models import Job

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Jobs:
    """V2 jobs resource"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def list(self) -> List[Job]:
        """List all available dataset jobs.

        Returns:
            List of :class:`Job` objects.
        """
        response = self._client._make_request(
            method="GET",
            endpoint="/jobs",
        )

        if response.status_code == 200:
            result: List[Dict[str, Any]] = response.json()
            return [Job.from_json(job) for job in result]
        else:
            raise Exception(
                f"Failed to list jobs: {response.status_code} - {response.text}"
            )
