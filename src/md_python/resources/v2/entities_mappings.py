"""
Entities mappings sub-resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class EntitiesMappings:
    """V2 entities mappings sub-resource"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def protein_to_protein(
        self, dataset_ids: List[str], entity_ids: List[str]
    ) -> Dict[str, Any]:
        """Map protein groups to protein groups through their shared individual proteins.

        Args:
            dataset_ids: List of dataset IDs, only protein groups from these datasets will be returned,
            entity_ids: List of protein group IDs to query

        Returns:
            Response dict with 'nodes' and 'edges' keys
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/mappings/protein_to_protein",
            json={"dataset_ids": dataset_ids, "entity_ids": entity_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to map protein_to_protein: {response.status_code} - {response.text}"
            )
