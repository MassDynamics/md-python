"""
Experiments resource for the MD Python client
"""

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from ..models import Experiment, SampleMetadata

if TYPE_CHECKING:
    from ..client import MDClient


class Experiments:
    """Experiments resource"""

    def __init__(self, client: "MDClient"):
        self._client = client

    def _validate_create_experiment(self, experiment: Experiment) -> None:
        """Validate experiment data before creation

        Args:
            experiment: Experiment object to validate

        Raises:
            ValueError: If validation fails
        """
        if not experiment.file_location and not experiment.s3_bucket:
            raise ValueError(
                "Either file_location or s3_bucket must be provided to create an experiment"
            )

        if experiment.file_location and not experiment.filenames:
            raise ValueError(
                "filenames must be provided when using file_location"
            )

    def _calculate_file_sizes(self, filenames: List[str], file_location: str) -> List[int]:
        """Calculate file sizes in bytes for given filenames

        Args:
            filenames: List of filenames to calculate sizes for
            file_location: Local directory path where files are located

        Returns:
            List of file sizes in bytes

        Raises:
            FileNotFoundError: If any file is not found
        """
        file_sizes = []
        for filename in filenames:
            file_path = os.path.join(file_location, filename)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            file_sizes.append(os.path.getsize(file_path))
        return file_sizes

    def _upload_single_file(self, url: str, file_path: str, filename: str) -> None:
        """Upload a single file to a presigned URL

        Args:
            url: Presigned URL for upload
            file_path: Local path to the file
            filename: Name of the file (for error messages)

        Raises:
            Exception: If upload fails
        """
        with open(file_path, "rb") as f:
            upload_response = requests.put(url, data=f)

        if upload_response.status_code not in [200, 204]:
            raise Exception(
                f"Failed to upload {filename}: {upload_response.status_code} - {upload_response.text}"
            )

    def _upload_multipart_file(
        self, parts: List[Dict[str, Any]], file_path: str, filename: str
    ) -> List[Dict[str, Any]]:
        """Upload a file using multipart upload

        Args:
            parts: List of part dictionaries containing url and part_number
            file_path: Local path to the file
            filename: Name of the file (for error messages)

        Returns:
            List of part responses with ETag headers

        Raises:
            Exception: If upload fails
        """
        file_size = os.path.getsize(file_path)
        num_parts = len(parts)
        part_size = file_size // num_parts
        last_part_size = file_size - (part_size * (num_parts - 1))

        uploaded_parts = []
        with open(file_path, "rb") as f:
            for part in sorted(parts, key=lambda x: x["part_number"]):
                part_number = part["part_number"]
                url = part["url"]

                if part_number == num_parts:
                    chunk_size = last_part_size
                else:
                    chunk_size = part_size

                chunk_data = f.read(chunk_size)
                upload_response = requests.put(url, data=chunk_data)

                if upload_response.status_code not in [200, 204]:
                    raise Exception(
                        f"Failed to upload part {part_number} of {filename}: {upload_response.status_code} - {upload_response.text}"
                    )

                etag = upload_response.headers.get("ETag", "").strip('"')
                uploaded_parts.append({"part_number": part_number, "etag": etag})

        return uploaded_parts

    def _complete_multipart_upload(
        self, experiment_id: str, filename: str, upload_session_id: str
    ) -> None:
        """Complete a multipart upload

        Args:
            experiment_id: ID of the experiment
            filename: Name of the file being uploaded
            upload_session_id: Upload session ID from multipart upload initiation

        Raises:
            Exception: If completion fails
        """
        response = self._client._make_request(
            method="POST",
            endpoint=f"/experiments/{experiment_id}/uploads/complete",
            json={"filename": filename, "upload_id": upload_session_id},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to complete multipart upload for {filename}: {response.status_code} - {response.text}"
            )

    def _upload_files(
        self, uploads: List[Dict[str, Any]], file_location: str, experiment_id: str
    ) -> None:
        """Upload files to presigned URLs, handling both single and multipart uploads

        Args:
            uploads: List of upload dictionaries containing filename, mode, and upload details
            file_location: Local directory path where files are located
            experiment_id: ID of the experiment (for completing multipart uploads)
        """
        for upload in uploads:
            filename = upload["filename"]
            mode = upload.get("mode", "single")
            file_path = os.path.join(file_location, filename)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            if mode == "multipart":
                upload_session_id = upload["upload_session_id"]
                parts = upload["parts"]
                self._upload_multipart_file(parts, file_path, filename)
                self._complete_multipart_upload(experiment_id, filename, upload_session_id)
            else:
                url = upload["url"]
                self._upload_single_file(url, file_path, filename)

    def create(self, experiment: Experiment) -> str:
        """Create a new experiment using Experiment model"""

        self._validate_create_experiment(experiment)

        experiment_payload: Dict[str, Any] = {
            "name": experiment.name,
            "description": experiment.description,
            "experiment_design": (
                experiment.experiment_design.data
                if experiment.experiment_design
                else None
            ),
            "labelling_method": experiment.labelling_method,
            "source": experiment.source,
            "filenames": experiment.filenames,
            "sample_metadata": (
                experiment.sample_metadata.data
                if experiment.sample_metadata
                else None
            ),
        }

        # decide how we deal with files, either uploaded from local or an existing S3 bucket
        if experiment.file_location:
            experiment_payload["file_location"] = experiment.file_location
            if experiment.filenames:
                file_sizes = self._calculate_file_sizes(experiment.filenames, experiment.file_location)
                experiment_payload["file_sizes"] = file_sizes
        else:
            experiment_payload["s3_bucket"] = experiment.s3_bucket
            experiment_payload["s3_prefix"] = experiment.s3_prefix

        payload = {"experiment": experiment_payload}

        response = self._client._make_request(
            method="POST",
            endpoint="/experiments",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200 or response.status_code == 201:
            response_data = response.json()
            experiment_id = str(response_data["id"])

            if "uploads" in response_data and experiment.file_location:
                self._upload_files(response_data["uploads"], experiment.file_location, experiment_id)
                print("Starting workflow")
                response = self._client._make_request(
                        method="POST",
                        endpoint=f"/experiments/{experiment_id}/start_workflow",
                        headers={"Content-Type": "application/json"},
                    )

            return experiment_id
        else:
            raise Exception(
                f"Failed to create experiment: {response.status_code} - {response.text}"
            )

    def get_by_name(self, name: str) -> Optional[Experiment]:
        """Get an experiment by its name, returns Experiment object"""

        response = self._client._make_request(
            method="GET", endpoint=f"/experiments?name={name}"
        )

        if response.status_code == 200:
            experiment_data = response.json()

            return Experiment.from_json(experiment_data)
        else:
            raise Exception(
                f"Failed to get experiment by name: {response.status_code} - {response.text}"
            )

    def get_by_id(self, experiment_id: str) -> Optional[Experiment]:
        """Get an experiment by its ID, returns Experiment object"""

        response = self._client._make_request(
            method="GET", endpoint=f"/experiments/{experiment_id}"
        )

        if response.status_code == 200:
            experiment_data = response.json()

            return Experiment.from_json(experiment_data)
        else:
            raise Exception(
                f"Failed to get experiment: {response.status_code} - {response.text}"
            )

    def update_sample_metadata(
        self, experiment_id: str, sample_metadata: SampleMetadata
    ) -> bool:
        """Update experiment's sample metadata, returns success status"""

        payload = {"sample_metadata": sample_metadata.data}

        response = self._client._make_request(
            method="PUT",
            endpoint=f"/experiments/{experiment_id}/sample_metadata",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "accept": "application/vnd.md-v1+json",
            },
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to update sample metadata: {response.status_code} - {response.text}"
            )

    def wait_until_complete(
        self, experiment_id: str, poll_s: int = 5, timeout_s: int = 1800
    ) -> Experiment:
        """Poll the experiment until it reaches a terminal state.

        Returns the latest Experiment object when terminal, or raises TimeoutError on timeout.
        Terminal states considered: COMPLETED, FAILED, ERROR, CANCELLED.
        """
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            exp = self.get_by_id(experiment_id)
            status = getattr(exp, "status", None)
            if status != last:
                print(f"status={status}")
                last = status

            if not status:
                time.sleep(poll_s)
                print("waiting for experiment to appear...")
                continue

            s = status.upper()
            if s in {"COMPLETED"}:
                return exp  # type: ignore[return-value]
            if s in {"FAILED", "ERROR", "CANCELLED"}:
                raise Exception(f"Experiment {experiment_id} failed: {status}")

            time.sleep(poll_s)

        raise TimeoutError(
            f"Experiment {experiment_id} not terminal within {timeout_s}s (last status={last})"
        )
