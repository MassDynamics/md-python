"""
Datasets resource for the MD Python client
"""

import time
from typing import TYPE_CHECKING, List, Dict, Any, Optional

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

    def wait_until_complete(
        self,
        experiment_id: str,
        dataset_id: str,
        poll_s: int = 5,
        timeout_s: int = 1800,
    ) -> Dict[str, Any]:
        """Poll the dataset until it reaches a terminal state using list_by_experiment.

        Returns the raw dataset dict when terminal, or raises TimeoutError on timeout.
        Terminal states considered: COMPLETED, FAILED, ERROR, CANCELLED.
        """
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            dds = self.list_by_experiment(experiment_id=experiment_id)
            ds = next((d for d in dds if str(d.id) == dataset_id), None)
            if ds:
                state = ds.state  # use dataset.state key
                if state != last:
                    print(f"state={state}")
                    last = state

                if state in {"COMPLETED"}:
                    return ds
                elif state in {"FAILED", "ERROR", "CANCELLED"}:
                    raise Exception(f"Dataset {dataset_id} failed: {state}")
            else:
                if last is None:
                    print("waiting for dataset to appear...")
            time.sleep(poll_s)

        raise TimeoutError(
            f"Dataset {dataset_id} not terminal within {timeout_s}s (last state={last})"
        )

    def find_initial_dataset(self, experiment_id: str) -> Optional[Dataset]:
        """Return the initial dataset for an experiment.

        Preference order:
        1) First dataset of type 'INTENSITY'
        2) Earliest by job_run_start_time
        3) First dataset if any
        """
        datasets = self.list_by_experiment(experiment_id=experiment_id)
        experiment_name = self._client.experiments.get_by_id(experiment_id).name

        if not datasets:
            raise ValueError(f"No datasets found for experiment {experiment_id}")

        intensity = [d for d in datasets if getattr(d, "type", None) == "INTENSITY"]
        if not intensity:
            raise ValueError(
                f"No intensity dataset found for experiment {experiment_id}"
            )

        by_name = [intd for intd in intensity if intd.name == experiment_name]
        if len(by_name) > 1:
            raise ValueError(
                f"Multiple intensity datasets found for experiment {experiment_id} with name {experiment_name}"
            )
        elif len(by_name) == 1:
            return by_name[0]
        else:
            raise ValueError(
                f"No initial dataset found for experiment {experiment_id} or name has been changed"
            )
