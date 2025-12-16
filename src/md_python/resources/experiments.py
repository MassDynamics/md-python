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

    def _upload_files(self, uploads: List[Dict[str, str]], file_location: str) -> None:
        """Upload files to presigned URLs

        Args:
            uploads: List of upload dictionaries containing filename and url
            file_location: Local directory path where files are located
        """
        for upload in uploads:
            filename = upload["filename"]
            url = upload["url"]
            file_path = os.path.join(file_location, filename)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            with open(file_path, "rb") as f:
                upload_response = requests.put(url, data=f)

            if upload_response.status_code not in [200, 204]:
                raise Exception(
                    f"Failed to upload {filename}: {upload_response.status_code} - {upload_response.text}"
                )

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
                self._upload_files(response_data["uploads"], experiment.file_location)
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
