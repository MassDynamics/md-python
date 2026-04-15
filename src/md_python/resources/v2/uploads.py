"""
Uploads resource for the MD Python v2 client
"""

import concurrent.futures
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...models import ExperimentDesign, SampleMetadata, Upload
from ...uploads import Uploads as FileUploader

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class Uploads:
    """V2 uploads resource — replaces v1 experiments"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client
        self._uploader = FileUploader(
            client, resource_path="/uploads", complete_path="/complete"
        )

    def create(
        self,
        upload: Upload,
        background: bool = False,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> str:
        """Create a new upload and optionally upload files.

        Args:
            upload: Upload object with upload configuration
            background: If True and file_location is set, transfer files in a
                background thread and return the upload ID immediately.
                The caller can then poll get_by_id() to check upload status.
                Default False preserves blocking behaviour for non-MCP use.

        Returns:
            Upload ID
        """
        if not upload.file_location and not upload.s3_bucket:
            raise ValueError("Either file_location or s3_bucket must be provided")

        if upload.file_location and not upload.filenames:
            raise ValueError("filenames must be provided when using file_location")

        if not upload.sample_metadata:
            raise ValueError("sample_metadata is required")

        payload: Dict[str, Any] = {
            "name": upload.name,
            "source": upload.source,
            "filenames": upload.filenames,
            "sample_metadata": upload.sample_metadata.data,
        }

        if upload.experiment_design:
            payload["experiment_design"] = upload.experiment_design.data

        if upload.file_location:
            payload["file_location"] = upload.file_location
            if upload.filenames:
                payload["file_sizes"] = self._uploader.file_sizes_for_api(
                    upload.filenames, upload.file_location
                )
        else:
            payload["s3_bucket"] = upload.s3_bucket
            payload["s3_prefix"] = upload.s3_prefix

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

        if "uploads" in response_data and upload.file_location:
            if background:
                if executor is not None:
                    executor.submit(
                        self._upload_and_start,
                        upload_id,
                        response_data["uploads"],
                        upload.file_location,
                    )
                else:
                    t = threading.Thread(
                        target=self._upload_and_start,
                        args=(
                            upload_id,
                            response_data["uploads"],
                            upload.file_location,
                        ),
                        daemon=True,
                    )
                    t.start()
            else:
                self._upload_and_start(
                    upload_id, response_data["uploads"], upload.file_location
                )

        return upload_id

    def _upload_and_start(
        self, upload_id: str, presigned_uploads: List[Any], file_location: str
    ) -> None:
        """Upload files to presigned URLs then start the processing workflow."""
        self._uploader.upload_files(presigned_uploads, file_location, upload_id)
        self._client._make_request(
            method="POST",
            endpoint=f"/uploads/{upload_id}/start_workflow",
            headers={"Content-Type": "application/json"},
        )

    def get_by_id(self, upload_id: str) -> Optional[Upload]:
        """Get an upload by its ID"""
        response = self._client._make_request(
            method="GET", endpoint=f"/uploads/{upload_id}"
        )

        if response.status_code == 200:
            return Upload.from_json(response.json())
        else:
            raise Exception(
                f"Failed to get upload: {response.status_code} - {response.text}"
            )

    def delete(self, upload_id: str) -> bool:
        """Delete an upload by ID"""
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/uploads/{upload_id}",
        )

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                f"Failed to delete upload: {response.status_code} - {response.text}"
            )

    def get_sample_metadata(self, upload_id: str) -> Optional[SampleMetadata]:
        """Get an upload's sample metadata"""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/uploads/{upload_id}/sample_metadata",
        )

        if response.status_code == 200:
            data = response.json()
            raw = data.get("sample_metadata")
            if raw is not None:
                return SampleMetadata(data=raw)
            return None
        else:
            raise Exception(
                f"Failed to get sample metadata: {response.status_code} - {response.text}"
            )

    def query(
        self,
        status: Optional[List[str]] = None,
        source: Optional[List[str]] = None,
        search: Optional[str] = None,
        sample_metadata: Optional[List[Dict[str, str]]] = None,
        page: int = 1,
    ) -> Dict[str, Any]:
        """Query uploads with filters"""
        payload: Dict[str, Any] = {"page": page}

        if status is not None:
            payload["status"] = status
        if source is not None:
            payload["source"] = source
        if search is not None:
            payload["search"] = search
        if sample_metadata is not None:
            payload["sample_metadata"] = sample_metadata

        response = self._client._make_request(
            method="POST",
            endpoint="/uploads/query",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to query uploads: {response.status_code} - {response.text}"
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
    ) -> Upload:
        """Poll the upload until it reaches a terminal state."""
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            upload = self.get_by_id(upload_id)
            status = getattr(upload, "status", None)
            if status != last:
                print(f"status={status}")
                last = status

            if not status:
                time.sleep(poll_s)
                continue

            s = status.upper()
            if s in {"COMPLETED"}:
                return upload  # type: ignore[return-value]
            if s in {"FAILED", "ERROR", "CANCELLED"}:
                raise Exception(f"Upload {upload_id} failed: {status}")

            time.sleep(poll_s)

        raise TimeoutError(
            f"Upload {upload_id} not terminal within {timeout_s}s (last status={last})"
        )
