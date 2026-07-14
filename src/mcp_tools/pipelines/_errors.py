"""Shared error-envelope helpers for the pipeline tools.

Pipeline tools surface LOCAL (pre-submission) validation failures as the prose
envelope ``"Error: <message>"`` rather than raising, so the calling LLM gets a
recovery path instead of an uncaught exception. Server-side failures (APIError)
still raise.
"""

from typing import List

from pydantic import ValidationError


def format_validation_error(exc: ValidationError) -> str:
    """Flatten a pydantic ValidationError into one LLM-readable line.

    The raw multi-line pydantic rendering (``1 validation error for
    NormalisationImputationDataset / experiment_design / Input should be a
    valid dictionary``) gives the caller no recovery path, so we surface the
    field name plus the message our own validators raised.
    """
    parts: List[str] = []
    for err in exc.errors():
        field = ".".join(str(loc) for loc in err["loc"]) or "<input>"
        msg = err["msg"].removeprefix("Value error, ")
        parts.append(msg if msg.startswith(field) else f"{field}: {msg}")
    return "; ".join(parts)
