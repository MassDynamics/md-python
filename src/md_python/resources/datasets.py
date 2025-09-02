"""
Datasets resource for the MD Python client
"""

from typing import TYPE_CHECKING, List

from ..models import Dataset

if TYPE_CHECKING:
    from ..client import MDClient


class Datasets:
    """Datasets resource"""

    def __init__(self, client: "MDClient"):
        self._client = client

    def create(self, dataset: Dataset) -> str:
        """Create a new dataset using Dataset model"""

        # Prepare the request payload
        payload = {
            "dataset": {
                "input_dataset_ids": [
                    str(dataset_id) for dataset_id in dataset.input_dataset_ids
                ],
                "name": dataset.name,
                "job_slug": dataset.job_slug,
                "job_run_params": dataset.job_run_params or {},
            }
        }

        # Make the API call
        response = self._client._make_request(
            method="POST",
            endpoint="/datasets",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "accept": "application/vnd.md-v1+json",
            },
        )

        if response.status_code == 200 or response.status_code == 201:
            response_data = response.json()
            return str(response_data["dataset_id"])
        else:
            raise Exception(
                f"Failed to create dataset: {response.status_code} - {response.text}"
            )

    def list_by_experiment(self, experiment_id: str) -> List[Dataset]:
        """Get datasets belonging to an experiment, returns list of Dataset objects"""

        # Make the API call with experiment_id query parameter
        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets?experiment_id={experiment_id}",
            headers={"accept": "application/vnd.md-v1+json"},
        )

        if response.status_code == 200:
            datasets_data = response.json()

            # Convert the API response to list of Dataset objects
            datasets = []
            for dataset_data in datasets_data:
                dataset = Dataset.from_json(dataset_data)
                datasets.append(dataset)

            return datasets
        else:
            raise Exception(
                f"Failed to get datasets by experiment: {response.status_code} - {response.text}"
            )

    def delete(self, dataset_id: str) -> bool:
        """Delete a dataset by ID

        Args:
            dataset_id: The ID of the dataset to delete

        Returns:
            bool: True if deletion was successful

        Raises:
            Exception: If the deletion fails
        """
        # Make the API call to delete the dataset
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/datasets/{dataset_id}",
            headers={"accept": "application/vnd.md-v1+json"},
        )

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                f"Failed to delete dataset: {response.status_code} - {response.text}"
            )

    def retry(self, dataset_id: str) -> bool:
        """Retry a failed dataset by ID

        Args:
            dataset_id: The ID of the dataset to retry

        Returns:
            bool: True if retry was initiated successfully

        Raises:
            Exception: If the retry fails
        """
        # Make the API call to retry the dataset
        response = self._client._make_request(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/retry",
            headers={"accept": "application/vnd.md-v1+json"},
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to retry dataset: {response.status_code} - {response.text}"
            )
