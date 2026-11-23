"""
Projects resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, List

from ...models import Project

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Projects:
    """V2 projects resource."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def list(self) -> List[Project]:
        """List the projects the current user can access.

        Organisation owners see every project in the organisation; other
        users see the projects they are a member of, directly or through a
        team. Each project includes the tag names of its data collection.

        Returns:
            A list of :class:`Project` objects, ordered by name.
        """
        response = self._client._make_request(method="GET", endpoint="/projects")

        if response.status_code == 200:
            return [Project.from_json(d) for d in response.json()]
        raise Exception(
            f"Failed to list projects: {response.status_code} - {response.text}"
        )
