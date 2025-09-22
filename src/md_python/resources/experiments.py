"""
Experiments resource for the MD Python client
"""

from typing import TYPE_CHECKING, Optional

from ..models import Experiment, SampleMetadata

if TYPE_CHECKING:
    from ..client import MDClient


class Experiments:
    """Experiments resource"""

    def __init__(self, client: "MDClient"):
        self._client = client

    def create(self, experiment: Experiment) -> str:
        """Create a new experiment using Experiment model"""

        # Prepare the request payload
        payload = {
            "experiment": {
                "name": experiment.name,
                "description": experiment.description,
                "experiment_design": (
                    experiment.experiment_design.data
                    if experiment.experiment_design
                    else None
                ),
                "labelling_method": experiment.labelling_method,
                "source": experiment.source,
                "s3_bucket": experiment.s3_bucket,
                "s3_prefix": experiment.s3_prefix,
                "filenames": experiment.filenames,
                "sample_metadata": (
                    experiment.sample_metadata.data
                    if experiment.sample_metadata
                    else None
                ),
            }
        }

        # Make the API call
        response = self._client._make_request(
            method="POST",
            endpoint="/experiments",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200 or response.status_code == 201:
            response_data = response.json()
            return str(response_data["id"])
        else:
            raise Exception(
                f"Failed to create experiment: {response.status_code} - {response.text}"
            )

    def get_by_name(self, name: str) -> Optional[Experiment]:
        """Get an experiment by its name, returns Experiment object"""

        # Make the API call with name query parameter
        response = self._client._make_request(
            method="GET", endpoint=f"/experiments?name={name}"
        )

        if response.status_code == 200:
            experiment_data = response.json()

            # Convert the API response to Experiment object
            return Experiment.from_json(experiment_data)
        else:
            raise Exception(
                f"Failed to get experiment by name: {response.status_code} - {response.text}"
            )

    def get_by_id(self, experiment_id: str) -> Optional[Experiment]:
        """Get an experiment by its ID, returns Experiment object"""

        # Make the API call
        response = self._client._make_request(
            method="GET", endpoint=f"/experiments/{experiment_id}"
        )

        if response.status_code == 200:
            experiment_data = response.json()

            # Convert the API response to Experiment object
            return Experiment.from_json(experiment_data)
        else:
            raise Exception(
                f"Failed to get experiment: {response.status_code} - {response.text}"
            )

    def update_sample_metadata(
        self, experiment_id: str, sample_metadata: SampleMetadata
    ) -> bool:
        """Update experiment's sample metadata, returns success status"""

        # Prepare the request payload
        payload = {"sample_metadata": sample_metadata.data}

        # Make the API call
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
