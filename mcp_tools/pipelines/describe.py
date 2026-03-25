"""describe_pipeline tool — returns the parameter schema for a pipeline type."""

import json

from .. import mcp
from ._schemas import _PIPELINE_SCHEMAS


@mcp.tool()
def describe_pipeline(job_slug: str) -> str:
    """Return the full parameter schema for a pipeline, including valid_values and defaults.

    Call this when you need to verify valid parameter values before running a pipeline.
    Not required if the parameter values are already known from context or prior calls.

    job_slug: one of "normalisation_imputation", "dose_response", "pairwise_comparison".
    Use list_jobs() to see all available slugs.
    """
    schema = _PIPELINE_SCHEMAS.get(job_slug)
    if schema is None:
        available = ", ".join(sorted(_PIPELINE_SCHEMAS))
        return f"Unknown job_slug '{job_slug}'. Known slugs with schemas: {available}"
    return json.dumps(schema, indent=2)
