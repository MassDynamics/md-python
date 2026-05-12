"""Pipeline tools for the Mass Dynamics MCP server.

Importing this package triggers @mcp.tool() registration for all pipeline tools.
"""

# Re-export private helper used by tests
from ._mandates import MANDATES_FRAGMENT, _attach
from ._metadata import _filter_sample_metadata
from .anova import run_anova
from .describe import describe_pipeline
from .dose_response import (
    run_dose_response,
    run_dose_response_bulk,
    run_dose_response_from_upload,
)
from .normalisation import (
    run_normalisation_imputation,
    run_normalisation_imputation_bulk,
)
from .pairwise import (
    generate_pairwise_comparisons,
    run_pairwise_comparison,
    run_pairwise_comparison_bulk,
)

# Append the binding LLM-behaviour mandates to every analysis tool's docstring.
# Single source of truth lives in ._mandates so future updates are easy.
_attach(
    run_anova,
    run_dose_response,
    run_dose_response_bulk,
    run_dose_response_from_upload,
    run_normalisation_imputation,
    run_normalisation_imputation_bulk,
    run_pairwise_comparison,
    run_pairwise_comparison_bulk,
)

__all__ = [
    "MANDATES_FRAGMENT",
    "describe_pipeline",
    "generate_pairwise_comparisons",
    "run_anova",
    "run_dose_response",
    "run_dose_response_bulk",
    "run_dose_response_from_upload",
    "run_normalisation_imputation",
    "run_normalisation_imputation_bulk",
    "run_pairwise_comparison",
    "run_pairwise_comparison_bulk",
]
