"""
Entities resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Entities:
    """V2 entities resource — search proteins/genes/peptides across datasets."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def search(self, keyword: str, dataset_ids: List[str]) -> List[Dict[str, Any]]:
        """Search for proteins, genes, or peptides by keyword across datasets.

        Args:
            keyword: Gene symbol, protein name, or UniProt ID (min 2 chars).
            dataset_ids: List of dataset IDs to search across (1–500).

        Returns:
            List of dicts with keys: dataset_id, entity_type, items.
            Each item has: ProteinIds, GeneNames, Description, GroupId.

        Raises:
            ValueError: on 400 (invalid params) or 404 (dataset not found).
            PermissionError: on 403 (feature not enabled on account).
            Exception: on 502 or other unexpected errors.
        """
        payload = {"keyword": keyword, "dataset_ids": dataset_ids}
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/search",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            results: List[Dict[str, Any]] = response.json().get("results", [])
            return results
        elif response.status_code in (400, 404):
            raise ValueError(
                f"Entity search failed ({response.status_code}): {response.text}"
            )
        elif response.status_code == 403:
            raise PermissionError(
                "Entity search is not enabled on your account. "
                "Contact support to enable entity search."
            )
        else:
            raise Exception(
                f"Entity search failed: {response.status_code} - {response.text}"
            )
