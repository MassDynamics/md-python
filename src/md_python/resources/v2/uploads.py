"""
Uploads resource for the MD Python v2 client
"""

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...models import Experiment, SampleMetadata
from ...uploads import Uploads as FileUploader

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Uploads:
    """V2 uploads resource — replaces v1 experiments"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client
        self._uploader = FileUploader(client, resource_path="/uploads")

    def create(self, experiment: Experiment) -> str:
        """Create a new upload and optionally upload files.

        Args:
            experiment: Experiment object with upload configuration

        Returns:
            Upload ID (experiment UUID)
        """
        if not experiment.file_location and not experiment.s3_bucket:
            raise ValueError(
                "Either file_location or s3_bucket must be provided"
            )

        if experiment.file_location and not experiment.filenames:
            raise ValueError("filenames must be provided when using file_location")

        payload: Dict[str, Any] = {
            "name": experiment.name,
            "source": experiment.source,
            "filenames": experiment.filenames,
        }

        if experiment.file_location:
            payload["file_location"] = experiment.file_location
            if experiment.filenames:
                file_sizes = self._uploader.file_sizes_for_api(
                    experiment.filenames, experiment.file_location
                )
                payload["file_sizes"] = file_sizes
        else:
            payload["s3_bucket"] = experiment.s3_bucket
            payload["s3_prefix"] = experiment.s3_prefix

        response = self._client._make_request(
            method="POST",
            endpoint="/uploads",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code not in (200, 201):
            raise Exception(
                f"Failed to create upload: {response.status_code} - {response.text}"
            )

        response_data = response.json()
        upload_id = str(response_data["id"])

        if "uploads" in response_data and experiment.file_location:
            self._uploader.upload_files(
                response_data["uploads"], experiment.file_location, upload_id
            )
            self._client._make_request(
                method="POST",
                endpoint=f"/uploads/{upload_id}/start_workflow",
                headers={"Content-Type": "application/json"},
            )

        return upload_id

    def get_by_id(self, upload_id: str) -> Optional[Experiment]:
        """Get an upload by its ID"""
        response = self._client._make_request(
            method="GET", endpoint=f"/uploads/{upload_id}"
        )

        if response.status_code == 200:
            return Experiment.from_json(response.json())
        else:
            raise Exception(
                f"Failed to get upload: {response.status_code} - {response.text}"
            )

    def get_by_name(self, name: str) -> Optional[Experiment]:
        """Get an upload by its name"""
        response = self._client._make_request(
            method="GET", endpoint=f"/uploads?name={name}"
        )

        if response.status_code == 200:
            return Experiment.from_json(response.json())
        else:
            raise Exception(
                f"Failed to get upload by name: {response.status_code} - {response.text}"
            )

    def update_sample_metadata(
        self, upload_id: str, sample_metadata: SampleMetadata
    ) -> bool:
        """Update an upload's sample metadata"""
        response = self._client._make_request(
            method="PUT",
            endpoint=f"/uploads/{upload_id}/sample_metadata",
            json={"sample_metadata": sample_metadata.data},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to update sample metadata: {response.status_code} - {response.text}"
            )

    def wait_until_complete(
        self, upload_id: str, poll_s: int = 5, timeout_s: int = 1800
    ) -> Experiment:
        """Poll the upload until it reaches a terminal state."""
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            exp = self.get_by_id(upload_id)
            status = getattr(exp, "status", None)
            if status != last:
                print(f"status={status}")
                last = status

            if not status:
                time.sleep(poll_s)
                continue

            s = status.upper()
            if s in {"COMPLETED"}:
                return exp  # type: ignore[return-value]
            if s in {"FAILED", "ERROR", "CANCELLED"}:
                raise Exception(f"Upload {upload_id} failed: {status}")

            time.sleep(poll_s)

        raise TimeoutError(
            f"Upload {upload_id} not terminal within {timeout_s}s (last status={last})"
        )
