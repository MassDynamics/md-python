"""Wait for a single pipeline dataset and fetch dataset state."""

import contextlib
import io
from typing import Dict

from .. import mcp
from .._client import get_client


def _fetch_dataset_state(job: Dict[str, str]) -> Dict[str, str]:
    """Fetch the current state of one dataset. Returns job dict augmented with 'state'.

    State is one of the API states (COMPLETED, RUNNING, FAILED, etc.) on success.
    Returns state="FETCH_ERROR" with an "error" key when the API call fails.

    upload_id is optional. When omitted, the dataset is fetched directly by ID
    (GET /datasets/:id). When provided, list_by_upload is used instead — useful
    when the direct endpoint is unavailable or for batching.
    """
    dataset_id = job["dataset_id"]
    upload_id = job.get("upload_id")
    try:
        if upload_id:
            datasets = get_client().datasets.list_by_upload(upload_id)
            ds = next((d for d in datasets if str(d.id) == dataset_id), None)
            if ds is None:
                return {
                    "dataset_id": dataset_id,
                    "upload_id": upload_id,
                    "state": "NOT_FOUND",
                    "error": "Dataset ID not found in upload — it may still be queued or the ID may be wrong.",
                }
        else:
            ds = get_client().datasets.get_by_id(dataset_id)
        return {"dataset_id": dataset_id, "state": ds.state}
    except Exception as e:
        result: Dict[str, str] = {
            "dataset_id": dataset_id,
            "state": "FETCH_ERROR",
            "error": f"API call failed: {e}",
        }
        if upload_id:
            result["upload_id"] = upload_id
        return result


@mcp.tool()
def wait_for_dataset(
    upload_id: str,
    dataset_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check pipeline dataset status, polling until a terminal state or timeout.

    Args:
        upload_id: the upload UUID the dataset belongs to.
        dataset_id: the dataset UUID returned by a run_* tool.
        poll_seconds: seconds between status checks (default 5).
        timeout_seconds: max seconds to wait before returning current status (default 45).
            Keep below 60 — the MCP client enforces a hard 60-second per-call limit.
            If timeout is reached, call again to continue monitoring.

    Terminal states (stops polling): COMPLETED, FAILED, ERROR, CANCELLED.
    Non-terminal — call again, this is normal, not stalled:
      RUNNING, PENDING, QUEUED.
      Proteomics pipelines typically complete in 10–40 minutes. RUNNING or
      PENDING immediately after submission is expected — do NOT report it as
      a failure or alert the user. Only FAILED or ERROR require action.

    On COMPLETED: use dataset_id as input_dataset_ids for the next pipeline.
    On FAILED/ERROR: call retry_dataset(dataset_id) to re-run.
    """
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            ds = get_client().datasets.wait_until_complete(
                upload_id, dataset_id, poll_s=poll_seconds, timeout_s=timeout_seconds
            )
        return str(ds)
    except TimeoutError:
        # Return current state — caller should call again to continue monitoring
        try:
            datasets = get_client().datasets.list_by_upload(upload_id)
            ds = next((d for d in datasets if str(d.id) == dataset_id), None)
            if ds:
                return (
                    f"State: {ds.state}. Pipeline not yet complete — "
                    f"call wait_for_dataset again to continue monitoring.\n{ds}"
                )
            return "Dataset not yet visible — call wait_for_dataset again to continue monitoring."
        except Exception as e:
            return f"State unknown (could not fetch dataset): {e}. Call wait_for_dataset again."
    except Exception as e:
        return f"Dataset {dataset_id} failed: {e}"
