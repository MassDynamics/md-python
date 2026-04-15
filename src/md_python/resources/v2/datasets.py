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

    def list_by_upload(self, upload_id: str) -> List[Dataset]:
        """Get datasets belonging to an upload"""
        response = self._client._make_request(
            method="POST",
            endpoint="/datasets/query",
            json={"upload_id": upload_id},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return [Dataset.from_json(d) for d in response.json().get("data", [])]
        else:
            raise Exception(
                f"Failed to get datasets: {response.status_code} - {response.text}"
            )

    def get_by_id(self, dataset_id: str) -> Optional[Dataset]:
        """Get a single dataset by ID"""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets/{dataset_id}",
        )

        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise Exception(
                f"Failed to get dataset: {response.status_code} - {response.text}"
            )
        return Dataset.from_json(response.json())

    def download_table_url(
        self, dataset_id: str, table_name: str, format: str = "csv"
    ) -> str:
        """Get a presigned download URL for a dataset table.

        The API returns a 302 redirect to a presigned URL.
        """
        if format not in ("csv", "parquet"):
            raise ValueError(f"format must be 'csv' or 'parquet', got '{format}'")

        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets/{dataset_id}/tables/{table_name}.{format}",
            allow_redirects=False,
        )

        if response.status_code == 302:
            location = response.headers.get("Location")
            if location:
                return location
            raise Exception("302 response missing Location header")
        else:
            raise Exception(
                f"Failed to get download URL: {response.status_code} - {response.text}"
            )

    def query(
        self,
        upload_id: Optional[str] = None,
        state: Optional[List[str]] = None,
        type: Optional[List[str]] = None,
        search: Optional[str] = None,
        page: int = 1,
    ) -> Dict[str, Any]:
        """Query datasets with filters"""
        payload: Dict[str, Any] = {"page": page}

        if upload_id is not None:
            payload["upload_id"] = upload_id
        if state is not None:
            payload["state"] = state
        if type is not None:
            payload["type"] = type
        if search is not None:
            payload["search"] = search

        response = self._client._make_request(
            method="POST",
            endpoint="/datasets/query",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to query datasets: {response.status_code} - {response.text}"
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
        """Poll the dataset until it reaches a terminal state.

        upload_id is retained for backwards compatibility but is no longer
        used — lookup now goes via get_by_id(dataset_id) directly so the
        caller does not need to know which upload owns the dataset and the
        poll is not capped by the first page of list_by_upload.
        """
        del upload_id  # unused; see docstring
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            ds = self.get_by_id(dataset_id)
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
