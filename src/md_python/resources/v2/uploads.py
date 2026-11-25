"""
Uploads resource for the MD Python v2 client
"""

import concurrent.futures
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...models import ExperimentDesign, SampleMetadata, Upload
from ...models.upload import Source, Status
from ...uploads import Uploads as FileUploader

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


# Authoritative server-side allow-list. Mirrors VALID_SOURCE_FORMATS in the
# Rails app (workflow/app/models/experiment.rb:27-34). Any value outside this
# set is rejected by the Rails create-time validator (experiment.rb:68-72),
# so the client refuses them up front to give the caller a clearer error.
#
# Deliberately excluded (all server-rejected): raw, diann_raw, generic_format,
# simple, unknown, diann_matrix, md_diann_maxlfq, msfragger. See the commit
# that introduced this constant for the per-value rationale.
ALLOWED_UPLOAD_SOURCES: frozenset[str] = frozenset(
    {
        "maxquant",
        "diann_tabular",
        "tims_diann",
        "spectronaut",
        "md_format",
        "md_format_gene",
    }
)


# The upload agent call blocks until the agent finishes (the server allows up
# to 15 minutes), so the client waits a little longer than that.
UPLOAD_AGENT_TIMEOUT_S = 16 * 60


class Uploads:
    """V2 uploads resource — replaces v1 experiments"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client
        self._uploader = FileUploader(
            client, resource_path="/uploads", complete_path="/complete"
        )
        self._agent_uploader = FileUploader(
            client,
            resource_path="/uploads/upload_agent_run",
            complete_path="/file_upload_complete",
            complete_method="PUT",
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
        if upload.source not in ALLOWED_UPLOAD_SOURCES:
            allowed = ", ".join(sorted(ALLOWED_UPLOAD_SOURCES))
            raise ValueError(
                f"source={upload.source!r} is not a supported upload format. "
                f"Valid values: {allowed}."
            )

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

        if upload.description is not None:
            payload["description"] = upload.description

        if upload.labelling_method is not None:
            payload["labelling_method"] = upload.labelling_method

        if upload.experiment_design:
            payload["experiment_design"] = upload.experiment_design.data

        if upload.tags is not None:
            payload["tags"] = upload.tags

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

    def run_upload_agent(
        self,
        name: str,
        file_location: str,
        filenames: List[str],
        description: Optional[str] = None,
        timeout: float = UPLOAD_AGENT_TIMEOUT_S,
    ) -> Dict[str, Any]:
        """Upload local files and have the AI upload agent create the upload.

        The agent inspects the files, works out the source and experiment
        design, creates the upload and starts its workflow as the calling
        user. This uploads the files, then blocks until the agent finishes,
        which can take several minutes. Requires the ``upload_agent`` feature
        flag; without it the server answers 404.

        Args:
            name: Name for the new upload.
            file_location: Local directory containing the files.
            filenames: Bare, unique names of the files in ``file_location``.
            description: Optional notes about the data, passed to the agent.
            timeout: Seconds to wait for the agent once the files are uploaded.

        Returns:
            The agent's result: ``id`` (the run id), ``status`` ("SUCCEEDED"
            or "FAILED"), ``upload_id``, ``source``, ``failure_reason`` and
            ``message``. A run the agent could not complete is returned (not
            raised) with ``status == "FAILED"``.
        """
        if not name or not name.strip():
            raise ValueError("name is required")
        if not filenames or len(set(filenames)) != len(filenames):
            raise ValueError("filenames must be a non-empty list of unique names")
        for filename in filenames:
            if (
                not filename
                or filename == "."
                or any(part in filename for part in ("/", "\\", "..", "\0"))
            ):
                raise ValueError(f"filename {filename!r} must be a bare file name")

        response = self._client._make_request(
            method="POST",
            endpoint="/uploads/upload_agent_run",
            json={
                "filenames": filenames,
                "file_sizes": self._agent_uploader.file_sizes_for_api(
                    filenames, file_location
                ),
            },
            headers={"Content-Type": "application/json"},
        )
        if response.status_code not in (200, 201):
            raise Exception(
                f"Failed to create upload agent run: {response.status_code} - {response.text}"
            )
        created = response.json()
        run_id = str(created["id"])

        self._agent_uploader.upload_files(
            created["file_uploads"], file_location, run_id
        )

        payload: Dict[str, Any] = {
            "status": "UPLOAD_COMPLETE",
            "name": name,
            "filenames": filenames,
        }
        if description is not None:
            payload["description"] = description

        response = self._client._make_request(
            method="PUT",
            endpoint=f"/uploads/upload_agent_run/{run_id}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        raise Exception(
            f"Failed to run upload agent {run_id}: {response.status_code} - {response.text}"
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
        status: Optional[List[Status]] = None,
        source: Optional[List[Source]] = None,
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

    def update(
        self,
        upload_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        experiment_design: Optional[ExperimentDesign] = None,
    ) -> bool:
        """Update an upload's name, description and/or experiment_design.

        At least one field must be supplied; any field left as ``None`` is
        omitted from the request and left unchanged server-side. Pass
        ``description=""`` to clear the description.

        ``experiment_design`` can only be updated while the upload is in the
        ``draft`` status (before ``start_workflow`` has been called); the
        server rejects the change with 409 once the workflow has started.

        Args:
            upload_id: Upload UUID.
            name: New name. Must be unique within the organization.
            description: New description; pass "" to clear it.
            experiment_design: New experiment design.

        Returns:
            True if the update succeeded.
        """
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if experiment_design is not None:
            payload["experiment_design"] = experiment_design.data

        if not payload:
            raise ValueError(
                "At least one of name, description or experiment_design "
                "must be supplied"
            )

        response = self._client._make_request(
            method="PUT",
            endpoint=f"/uploads/{upload_id}",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to update upload: {response.status_code} - {response.text}"
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
            if s == "DRAFT":
                raise Exception(
                    f"Upload {upload_id} is in draft status; call start_workflow "
                    "before waiting for it to complete"
                )
            if s in {"COMPLETED"}:
                return upload  # type: ignore[return-value]
            if s in {"FAILED", "ERROR", "CANCELLED", "PROCESSING_FAILED"}:
                raise Exception(f"Upload {upload_id} failed: {status}")

            time.sleep(poll_s)

        raise TimeoutError(
            f"Upload {upload_id} not terminal within {timeout_s}s (last status={last})"
        )
