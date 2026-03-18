from typing import List, Optional

from md_python.models.metadata import ExperimentDesign, SampleMetadata
from md_python.models.upload import Upload

from . import mcp
from ._client import get_client


@mcp.tool()
def get_upload(
    upload_id: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Get an upload by ID or name.

    Provide either upload_id (UUID string) or name — not both.
    Returns upload details including status, source, and metadata.
    """
    if not upload_id and not name:
        return "Error: provide either upload_id or name"
    c = get_client()
    upload = (
        c.uploads.get_by_id(upload_id) if upload_id else c.uploads.get_by_name(name)
    )
    return str(upload) if upload else "Upload not found"


@mcp.tool()
def create_upload(
    name: str,
    source: str,
    experiment_design: List[List[str]],
    sample_metadata: List[List[str]],
    s3_bucket: Optional[str] = None,
    s3_prefix: Optional[str] = None,
    filenames: Optional[List[str]] = None,
    file_location: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Create a new upload and trigger the processing workflow.

    experiment_design: 2D array — first row is header [filename, sample_name, condition],
    subsequent rows are data rows.

    sample_metadata: 2D array — first row is header (e.g. [sample_name, dose]),
    subsequent rows are data rows. Required.

    For S3-backed uploads: provide s3_bucket, s3_prefix, and filenames.
    For local file uploads: provide file_location (directory path) and filenames.

    Returns the new upload ID on success.
    """
    upload = Upload(
        name=name,
        source=source,
        description=description,
        experiment_design=ExperimentDesign(data=experiment_design),
        sample_metadata=SampleMetadata(data=sample_metadata),
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        filenames=filenames,
        file_location=file_location,
    )
    upload_id = get_client().uploads.create(upload)
    return f"Upload created. ID: {upload_id}"


@mcp.tool()
def update_sample_metadata(
    upload_id: str,
    sample_metadata: List[List[str]],
) -> str:
    """Update the sample metadata for an existing upload.

    sample_metadata: 2D array — first row is header (e.g. [sample_name, dose, ...]),
    subsequent rows are data rows.
    """
    ok = get_client().uploads.update_sample_metadata(
        upload_id, SampleMetadata(data=sample_metadata)
    )
    return (
        "Sample metadata updated successfully"
        if ok
        else "Failed to update sample metadata"
    )


@mcp.tool()
def wait_for_upload(
    upload_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 1800,
) -> str:
    """Poll an upload until it reaches a terminal state (COMPLETED, FAILED, ERROR, CANCELLED).

    Returns the final upload status and details. Default timeout is 30 minutes.
    """
    upload = get_client().uploads.wait_until_complete(
        upload_id, poll_s=poll_seconds, timeout_s=timeout_seconds
    )
    return str(upload)
