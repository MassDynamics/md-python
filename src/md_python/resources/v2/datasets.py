"""
Datasets resource for the MD Python v2 client
"""

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...models import Dataset

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Datasets:
    """V2 datasets resource — flat payload, no wrapper"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def create(self, dataset: Dataset) -> str:
        """Create a new dataset.

        V2 uses a flat payload (no wrapping 'dataset' key).

        Args:
            dataset: Dataset object with creation parameters

        Returns:
            Created dataset ID
        """
        payload: Dict[str, Any] = {
            "input_dataset_ids": [
                str(dataset_id) for dataset_id in dataset.input_dataset_ids
            ],
            "name": dataset.name,
            "job_slug": dataset.job_slug,
            "job_run_params": dataset.job_run_params or {},
        }

        response = self._client._make_request(
            method="POST",
            endpoint="/datasets",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code in (200, 201):
            return str(response.json()["dataset_id"])
        else:
            raise Exception(
                f"Failed to create dataset: {response.status_code} - {response.text}"
            )

    def get_by_id(self, dataset_id: str) -> Dataset:
        """Get a single dataset by its ID."""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets/{dataset_id}",
        )

        if response.status_code == 200:
            return Dataset.from_json(response.json())
        else:
            raise Exception(
                f"Failed to get dataset: {response.status_code} - {response.text}"
            )

    def list_by_upload(self, upload_id: str) -> List[Dataset]:
        """Get datasets belonging to an upload"""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets?experiment_id={upload_id}",
        )

        if response.status_code == 200:
            return [Dataset.from_json(d) for d in response.json()]
        else:
            raise Exception(
                f"Failed to get datasets: {response.status_code} - {response.text}"
            )

    def delete(self, dataset_id: str) -> bool:
        """Delete a dataset by ID"""
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/datasets/{dataset_id}",
        )

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                f"Failed to delete dataset: {response.status_code} - {response.text}"
            )

    def retry(self, dataset_id: str) -> bool:
        """Retry a failed dataset"""
        response = self._client._make_request(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/retry",
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to retry dataset: {response.status_code} - {response.text}"
            )

    def cancel(self, dataset_id: str) -> bool:
        """Cancel a processing dataset"""
        response = self._client._make_request(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/cancel",
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to cancel dataset: {response.status_code} - {response.text}"
            )

    def wait_until_complete(
        self,
        upload_id: str,
        dataset_id: str,
        poll_s: int = 5,
        timeout_s: int = 1800,
    ) -> Dataset:
        """Poll the dataset until it reaches a terminal state."""
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            dds = self.list_by_upload(upload_id=upload_id)
            ds = next((d for d in dds if str(d.id) == dataset_id), None)
            if ds:
                state = ds.state
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

    def find_initial_dataset(self, upload_id: str) -> Optional[Dataset]:
        """Return the initial dataset for an upload."""
        datasets = self.list_by_upload(upload_id=upload_id)

        if not datasets:
            raise ValueError(f"No datasets found for upload {upload_id}")

        intensity = [d for d in datasets if getattr(d, "type", None) == "INTENSITY"]
        if not intensity:
            raise ValueError(f"No intensity dataset found for upload {upload_id}")

        if len(intensity) == 1:
            return intensity[0]

        raise ValueError(f"Multiple intensity datasets found for upload {upload_id}")
