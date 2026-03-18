from typing import List, Optional

from md_python.models.experiment import Experiment
from md_python.models.metadata import ExperimentDesign, SampleMetadata

from . import mcp
from ._client import get_client


@mcp.tool()
def get_experiment(
    experiment_id: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Get an experiment by ID or name.

    Provide either experiment_id (UUID string) or name — not both.
    Returns experiment details including status, source, and metadata.
    """
    if not experiment_id and not name:
        return "Error: provide either experiment_id or name"
    c = get_client()
    exp = (
        c.experiments.get_by_id(experiment_id)
        if experiment_id
        else c.experiments.get_by_name(name)
    )
    return str(exp) if exp else "Experiment not found"


@mcp.tool()
def create_experiment(
    name: str,
    source: str,
    experiment_design: List[List[str]],
    sample_metadata: Optional[List[List[str]]] = None,
    s3_bucket: Optional[str] = None,
    s3_prefix: Optional[str] = None,
    filenames: Optional[List[str]] = None,
    file_location: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Create a new experiment and trigger the processing workflow.

    experiment_design: 2D array — first row is header [filename, sample_name, condition],
    subsequent rows are data rows.

    sample_metadata: optional 2D array — first row is header (e.g. [sample_name, dose]),
    subsequent rows are data rows.

    For S3-backed experiments: provide s3_bucket, s3_prefix, and filenames.
    For local file uploads: provide file_location (directory path) and filenames.

    Returns the new experiment ID on success.
    """
    exp = Experiment(
        name=name,
        source=source,
        description=description,
        experiment_design=ExperimentDesign(data=experiment_design),
        sample_metadata=(
            SampleMetadata(data=sample_metadata) if sample_metadata else None
        ),
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        filenames=filenames,
        file_location=file_location,
    )
    experiment_id = get_client().experiments.create(exp)
    return f"Experiment created. ID: {experiment_id}"


@mcp.tool()
def update_sample_metadata(
    experiment_id: str,
    sample_metadata: List[List[str]],
) -> str:
    """Update the sample metadata for an existing experiment.

    sample_metadata: 2D array — first row is header (e.g. [sample_name, dose, ...]),
    subsequent rows are data rows.
    """
    ok = get_client().experiments.update_sample_metadata(
        experiment_id, SampleMetadata(data=sample_metadata)
    )
    return (
        "Sample metadata updated successfully"
        if ok
        else "Failed to update sample metadata"
    )


@mcp.tool()
def wait_for_experiment(
    experiment_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 1800,
) -> str:
    """Poll an experiment until it reaches a terminal state (COMPLETED, FAILED, ERROR, CANCELLED).

    Returns the final experiment status and details. Default timeout is 30 minutes.
    """
    exp = get_client().experiments.wait_until_complete(
        experiment_id, poll_s=poll_seconds, timeout_s=timeout_seconds
    )
    return str(exp)
