"""
Entities resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Entities:
    """V2 entities resource"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def query(self, keyword: str, dataset_ids: List[str]) -> Dict[str, Any]:
        """Query entity metadata across one or more datasets.

        Args:
            keyword: Search keyword (min 2 characters)
            dataset_ids: List of dataset IDs to search across

        Returns:
            Response dict with a 'results' key
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/query",
            json={"keyword": keyword, "dataset_ids": dataset_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to query entities: {response.status_code} - {response.text}"
            )
