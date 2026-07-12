import json

from . import mcp
from ._client import get_client


@mcp.tool()
def create_evosep_qc(filename: str, blob: dict) -> str:
    """Create an Evosep QC record from an uploaded QC file's JSON contents.

    Returns: JSON. Shape:
      {"id": "<uuid>", "filename": "<str>", "uploaded_by": "<str>",
       "created_at": "<iso8601>"}
    Field names are passed through verbatim from the server — parse
    defensively. On transport / HTTP failure returns {"error": "<message>"}.
    A feature-flag-off account produces an error whose message contains
    "404" and the body {"error":"Not found"} (see Args → feature flag);
    treat that as "the Evosep QC feature is not enabled for this account",
    NOT as a bug or a retryable transient.

    Use this when: the user has an Evosep QC file (its parsed JSON contents)
    and wants to persist a QC record for it on Mass Dynamics.

    Do NOT use this when: the Evosep QC feature is not enabled for the
    account (the tool will return a 404 error — do not retry, tell the user
    the feature is not enabled); when the user wants to upload proteomics
    RESULTS for analysis (use create_upload_from_csv / create_upload — this
    tool is only for Evosep instrument QC records, not experiment data);
    when you do not already have the file's parsed JSON contents (this tool
    does not read files — you must be handed ``blob`` as a dict).

    Args:
      filename: Name of the uploaded QC file. Required
        (workflow app/api/api/v2/evosep_qcs/create.rb — filename, required).
      blob: Arbitrary JSON contents of the QC file as a dict. Required
        (workflow app/api/api/v2/evosep_qcs/create.rb — blob, required). Any
        JSON-serialisable object is accepted; the server stores it verbatim.

      Feature flag: the endpoint is gated behind the Flipper flag
        ``evosep_qc``. If the flag is OFF for the caller the server returns
        404 {"error":"Not found"} and this tool returns
        {"error": "... 404 ..."}. That is an EXPECTED "feature not enabled
        for this account" state — surface it to the user, do not retry.
        A 422 with {"errors":[...]} indicates a validation failure on the
        submitted fields.

    Guardrails: this WRITES a record — it is a create / non-idempotent
    operation. Calling it twice with the same arguments creates TWO
    records (no server-side dedupe). It is NOT in DESTRUCTIVE_TOOL_NAMES
    (creating a record cannot destroy existing data and needs no
    delete-style confirmation), but because it mutates server state you
    should confirm the filename and that the user intends to persist the
    record before calling, and never fire it speculatively inside a batch
    of read-only tools.

    See also: create_upload_from_csv (for proteomics experiment data).
    """
    try:
        result = get_client().evosep_qcs.create(filename=filename, blob=blob)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
