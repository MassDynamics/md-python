"""Update an upload's name and/or description."""

from typing import Optional

from .. import mcp
from .._client import get_client


@mcp.tool()
def update_upload(
    upload_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Rename an upload and/or change its description.

    Returns: prose. "Upload updated successfully. ID: <id>" on 200 OK, or an
    "Error: ..." string on failure (per the mcp_tools.__init__ CONTRIBUTOR
    CONTRACT — prose tools surface failures with the "Error:" sentinel).
    NOT JSON — do not json.loads this.

    Partial update: whichever field you omit is left unchanged server-side.
    Pass description="" to CLEAR an existing description.

    Use this when: the user asks to rename an upload, fix a typo in its
    name, or add/correct its description after it was created.

    Do NOT use this when: the upload has not been created yet — pass name
    and description to create_upload[_from_csv] instead. This tool does NOT
    touch sample metadata, files, or any downstream dataset; it only edits
    the two free-text fields on the upload record. To change sample
    metadata use update_sample_metadata.

    Args:
      upload_id: UUID of the upload to update.
      name: New name. Must be UNIQUE within the organisation — reusing the
        name of another upload fails with a 422.
      description: New description. "" clears it.

    Errors:
      - "Error: provide at least one of name or description" — neither
        supplied (the server would 400).
      - "Error: an upload named '<name>' already exists ..." — 422, the name
        is taken within the organisation, or blank. The upload is UNCHANGED.
      - "Error: ..." — any other server failure.

    Guardrails:
      - Renaming is user-visible and has no undo. Only rename an upload the
        user explicitly asked to rename, and echo the old and new name back
        to them. Do NOT rename uploads opportunistically to "tidy" them.
      - The name is how the user finds the upload in the web app and via
        get_upload(name=...). A rename silently invalidates any name the
        user (or an earlier step in this conversation) is still holding.

    See also: get_upload, create_upload, update_sample_metadata.
    """
    if name is None and description is None:
        return "Error: provide at least one of name or description"

    try:
        upload = get_client().uploads.update(
            upload_id, name=name, description=description
        )
    except Exception as e:
        msg = str(e)
        if "422" in msg:
            return (
                f"Error: the update was rejected ({msg}). An upload name must be "
                "non-blank and unique within the organisation — pick a different "
                "name. The upload is unchanged."
            )
        return f"Error: {e}"

    if upload is None:
        return "Error: update_upload returned an unknown server response"
    return f"Upload updated successfully. ID: {upload_id}"
