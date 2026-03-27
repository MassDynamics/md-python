"""Wait for an upload to reach a terminal state."""

import contextlib
import io

from .. import mcp
from .._client import get_client


@mcp.tool()
def wait_for_upload(
    upload_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check upload status, polling until a terminal state or the timeout is reached.

    IMPORTANT — MCP CLIENT TIMEOUT: The MCP client enforces a hard 60-second limit
    per tool call. This tool defaults to 45 seconds so it fits within that cap.
    If the upload is still processing when the timeout is reached, this tool returns
    the current status instead of raising an error. Simply call it again to continue
    monitoring. A typical upload may require several calls over a few minutes.

    Terminal states (stops polling):
      COMPLETED  — data ingested; call find_initial_dataset next.
      FAILED / ERROR — ingestion failed; check the returned message for details.
      CANCELLED  — upload was stopped.

    Non-terminal (call again) — this is normal, not stalled:
      PROCESSING / PENDING — still in progress; call wait_for_upload again.
      File transfers for large experiments can take several minutes; server
      ingestion typically adds another 5–20 minutes on top of transfer time.
      Do NOT report PROCESSING/PENDING as a failure or alert the user — only
      FAILED or ERROR require action.

    For background file uploads started by create_upload_from_csv: the upload
    will initially show PENDING while files are transferring, then transition to
    PROCESSING once the server begins ingestion.
    """
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            upload = get_client().uploads.wait_until_complete(
                upload_id, poll_s=poll_seconds, timeout_s=timeout_seconds
            )
        return str(upload)
    except TimeoutError:
        # Return current status — caller should call again to continue monitoring
        try:
            upload = get_client().uploads.get_by_id(upload_id)
            status = getattr(upload, "status", "UNKNOWN")
            return (
                f"Status: {status}. Upload not yet complete — "
                f"call wait_for_upload again to continue monitoring.\n{upload}"
            )
        except Exception as e:
            return f"Status unknown (could not fetch upload): {e}. Call wait_for_upload again."
    except Exception as e:
        return f"Upload {upload_id} failed: {e}"
