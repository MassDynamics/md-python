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

    def protein_to_peptide_same_dataset(
        self, dataset_id: str, entity_ids: List[str]
    ) -> Dict[str, Any]:
        """Map protein groups to their peptides within a single dataset.

        Args:
            dataset_id: ID of the dataset that contains protein groups and peptides.
            entity_ids: List of protein groups to query within this dataset

        Returns:
            Response dict with 'nodes' and 'edges' keys
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/mappings/protein_to_peptide/same_dataset",
            json={"dataset_id": dataset_id, "entity_ids": entity_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to map protein_to_peptide_same_dataset: {response.status_code} - {response.text}"
            )

    def peptide_to_protein_same_dataset(
        self, dataset_id: str, entity_ids: List[str]
    ) -> Dict[str, Any]:
        """Map peptides to their protein groups within a single dataset.

        Args:
            dataset_id: ID of the dataset that contains peptides and protein groups.
            entity_ids: List of peptide IDs to query within this dataset

        Returns:
            Response dict with 'nodes' and 'edges' keys
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/mappings/peptide_to_protein/same_dataset",
            json={"dataset_id": dataset_id, "entity_ids": entity_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to map peptide_to_protein_same_dataset: {response.status_code} - {response.text}"
            )

    def gene_to_protein(
        self, dataset_ids: List[str], entity_ids: List[str]
    ) -> Dict[str, Any]:
        """Map gene entities to protein groups across one or more datasets.

        Returns nodes and edges reachable from the given gene IDs through
        their associated proteins to protein groups via the cross-reference.

        Args:
            dataset_ids: 1–500 dataset UUIDs to scope the graph to.
            entity_ids: Gene IDs to query (≥1).

        Returns:
            Response dict with 'nodes' and 'edges' keys.
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/mappings/gene_to_protein",
            json={"dataset_ids": dataset_ids, "entity_ids": entity_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to map gene_to_protein: {response.status_code} - {response.text}"
            )

    def protein_to_gene(
        self, dataset_ids: List[str], entity_ids: List[str]
    ) -> Dict[str, Any]:
        """Map protein groups to gene entities across one or more datasets.

        Returns nodes and edges reachable from the given protein-group IDs
        through their individual proteins to gene entities via the
        cross-reference.

        Args:
            dataset_ids: 1–500 dataset UUIDs to scope the graph to.
            entity_ids: Protein-group IDs to query (≥1).

        Returns:
            Response dict with 'nodes' and 'edges' keys.
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/entities/mappings/protein_to_gene",
            json={"dataset_ids": dataset_ids, "entity_ids": entity_ids},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to map protein_to_gene: {response.status_code} - {response.text}"
            )
