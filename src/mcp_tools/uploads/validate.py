"""Validate experiment_design and sample_metadata before upload."""

from collections import Counter
from typing import List

from .. import mcp


@mcp.tool()
def validate_upload_inputs(
    experiment_design: List[List[str]],
    sample_metadata: List[List[str]],
) -> str:
    """Validate experiment_design and sample_metadata before calling create_upload.

    Call this BEFORE create_upload to catch mismatches that would cause the upload to fail.

    What each table is:
      experiment_design — maps raw data files to biological samples.
        Required columns: filename (raw file name without extension),
        sample_name (unique sample label), condition (experimental group).
        One row per raw file. For LFQ data, filename usually equals sample_name.
      sample_metadata — per-sample experimental variables used by analysis pipelines.
        Required column: sample_name (must exactly match experiment_design).
        Optional columns: dose, batch, cellline, drug, or any covariate.

    Checks performed:
      - experiment_design has required columns: filename, sample_name, condition
      - sample_metadata has a sample_name column
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

    # Duplicate check (Counter is O(n) vs list.count's O(n²))
    ed_dupes = sorted(s for s, n in Counter(ed_samples).items() if n > 1)
    if ed_dupes:
        errors.append(f"Duplicate sample_names in experiment_design: {ed_dupes}")

    sm_dupes = sorted(s for s, n in Counter(sm_samples).items() if n > 1)
    if sm_dupes:
        errors.append(f"Duplicate sample_names in sample_metadata: {sm_dupes}")

    if errors:
        return "Validation failed:\n" + "\n".join(f"  - {e}" for e in errors)

    return f"OK: {len(ed_samples)} samples validated"
