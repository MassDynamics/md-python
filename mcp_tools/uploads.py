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
def validate_upload_inputs(
    experiment_design: List[List[str]],
    sample_metadata: List[List[str]],
) -> str:
    """Validate experiment_design and sample_metadata before calling create_upload.

    Call this BEFORE create_upload to catch mismatches that would cause the upload to fail.

    Checks:
    - experiment_design has required columns: filename, sample_name, condition
    - sample_metadata has a sample_name column (first column or named 'sample_name')
    - Every sample_name in experiment_design appears in sample_metadata (exact match, case-sensitive)
    - Every sample_name in sample_metadata appears in experiment_design (no orphans)
    - No duplicate sample_names in either table

    Returns "OK: N samples validated" on success, or a detailed error message.
    """
    errors = []

    # Validate experiment_design structure
    if not experiment_design or len(experiment_design) < 2:
        return (
            "Error: experiment_design must have a header row and at least one data row"
        )

    ed_header = [h.strip().lower() for h in experiment_design[0]]
    synonyms = {"file": "filename", "sample": "sample_name", "group": "condition"}
    ed_header_norm = [synonyms.get(h, h) for h in ed_header]

    for col in ["filename", "sample_name", "condition"]:
        if col not in ed_header_norm:
            errors.append(
                f"experiment_design missing required column '{col}' "
                f"(got: {experiment_design[0]})"
            )

    if errors:
        return "\n".join(errors)

    sample_idx = ed_header_norm.index("sample_name")
    ed_samples = [
        row[sample_idx]
        for row in experiment_design[1:]
        if isinstance(row, list) and len(row) > sample_idx
    ]

    # Validate sample_metadata structure
    if not sample_metadata or len(sample_metadata) < 2:
        return "Error: sample_metadata must have a header row and at least one data row"

    sm_header = [h.strip().lower() for h in sample_metadata[0]]
    if "sample_name" not in sm_header:
        errors.append(
            f"sample_metadata must have a 'sample_name' column; got: {sample_metadata[0]}"
        )
        return "\n".join(errors)

    sm_sample_idx = sm_header.index("sample_name")
    sm_samples = [
        row[sm_sample_idx]
        for row in sample_metadata[1:]
        if isinstance(row, list) and len(row) > sm_sample_idx
    ]

    # Cross-check
    ed_set = set(ed_samples)
    sm_set = set(sm_samples)

    missing_from_sm = ed_set - sm_set
    if missing_from_sm:
        errors.append(
            f"sample_names in experiment_design but NOT in sample_metadata: {sorted(missing_from_sm)}"
        )

    missing_from_ed = sm_set - ed_set
    if missing_from_ed:
        errors.append(
            f"sample_names in sample_metadata but NOT in experiment_design: {sorted(missing_from_ed)}"
        )

    # Duplicate check
    ed_dupes = [s for s in ed_set if ed_samples.count(s) > 1]
    if ed_dupes:
        errors.append(
            f"Duplicate sample_names in experiment_design: {sorted(set(ed_dupes))}"
        )

    sm_dupes = [s for s in sm_set if sm_samples.count(s) > 1]
    if sm_dupes:
        errors.append(
            f"Duplicate sample_names in sample_metadata: {sorted(set(sm_dupes))}"
        )

    if errors:
        return "Validation failed:\n" + "\n".join(f"  - {e}" for e in errors)

    return f"OK: {len(ed_samples)} samples validated"


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

    IMPORTANT: Call validate_upload_inputs first to catch errors before submission.

    experiment_design: 2D array. Header row must contain: filename, sample_name, condition
    (synonyms accepted: file=filename, sample=sample_name, group=condition).
    Extra columns are dropped — only those three are kept.
    Example: [["filename","sample_name","condition"],["sample1.raw","S1","treated"],...]

    sample_metadata: 2D array. Must have a 'sample_name' column whose values EXACTLY
    match (case-sensitive) all sample_name values in experiment_design — no more, no less.
    May include additional columns (dose, batch, etc.).
    Example: [["sample_name","dose"],["S1","10"],["S2","0"],...]

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
