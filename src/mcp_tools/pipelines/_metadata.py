"""Helpers for resolving and filtering sample metadata from upload records."""

from typing import List, Optional

from .._client import get_client


def _filter_sample_metadata(
    metadata: List[List[str]], sample_names: List[str]
) -> List[List[str]]:
    """Return header row + data rows whose sample_name is in sample_names."""
    if not metadata:
        return metadata
    header = metadata[0]
    try:
        sn_idx = [h.strip().lower() for h in header].index("sample_name")
    except ValueError:
        return metadata  # can't filter without sample_name column; return as-is
    sample_set = set(sample_names)
    return [header] + [
        row for row in metadata[1:] if len(row) > sn_idx and row[sn_idx] in sample_set
    ]


def _fetch_upload_sample_metadata(upload_id: str) -> Optional[List[List[str]]]:
    """Fetch sample_metadata from the upload record, or return None on failure."""
    try:
        upload = get_client().uploads.get_by_id(upload_id)
        if upload and upload.sample_metadata:
            return upload.sample_metadata.data
    except Exception:
        pass
    return None
