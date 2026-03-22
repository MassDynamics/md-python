import json

from . import mcp
from ._client import get_client


@mcp.tool()
def list_jobs() -> str:
    """List all available pipeline job types published on this Mass Dynamics instance.

    Returns job slugs, names, and descriptions. Use these slugs with
    describe_pipeline(<slug>) to inspect parameters, and then with
    run_normalisation_imputation, run_pairwise_comparison, or run_dose_response
    to execute them.

    Typical slugs: normalisation_imputation, pairwise_comparison, dose_response.
    """
    jobs = get_client().jobs.list()
    if not jobs:
        return "No jobs available"
    return json.dumps(jobs, indent=2)


@mcp.tool()
def list_datasets(upload_id: str) -> str:
    """List all datasets associated with an upload, with their IDs, names, types, and states.

    Use this for inspection or debugging. In a normal pipeline workflow, use
    find_initial_dataset instead — it returns the specific INTENSITY dataset ID
    needed as input for run_* pipeline tools.

    Dataset types you may see:
      INTENSITY   — the initial processed dataset (input for pipelines)
      PAIRWISE    — output of run_pairwise_comparison
      DOSE_RESPONSE — output of run_dose_response
    """
    datasets = get_client().datasets.list_by_upload(upload_id)
    if not datasets:
        return "No datasets found for this upload"
    lines = [f"Found {len(datasets)} dataset(s):"]
    for ds in datasets:
        lines.append(
            f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
        )
    return "\n".join(lines)


@mcp.tool()
def find_initial_dataset(upload_id: str) -> str:
    """Find the initial INTENSITY dataset for an upload.

    Call this after wait_for_upload returns COMPLETED. The dataset ID
    returned here is what you pass as input_dataset_ids to every run_*
    pipeline tool (run_normalisation_imputation, run_pairwise_comparison,
    run_dose_response).

    Returns the dataset ID and details on success, or an error if the upload
    has not finished processing yet.
    """
    ds = get_client().datasets.find_initial_dataset(upload_id)
    if not ds:
        return "No initial INTENSITY dataset found for this upload"
    return f"Initial dataset found.\nID: {ds.id}\n{ds}"


@mcp.tool()
def wait_for_dataset(
    upload_id: str,
    dataset_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check pipeline dataset status, polling until a terminal state or timeout.

    IMPORTANT — MCP CLIENT TIMEOUT: The MCP client enforces a hard 60-second limit
    per tool call. This tool defaults to 45 seconds so it fits within that cap.
    If the pipeline is still running when the timeout is reached, this tool returns
    the current status instead of raising an error. Simply call it again to continue
    monitoring. A pipeline run may require many calls over several minutes.

    Call this after run_normalisation_imputation, run_pairwise_comparison, or
    run_dose_response. Pass both the upload_id and the dataset_id returned by the
    run_* call.

    Terminal states (stops polling):
      COMPLETED — results are ready in the Mass Dynamics app.
                  Use this dataset_id as input_dataset_ids for the next pipeline.
      FAILED / ERROR — pipeline failed; call retry_dataset(dataset_id) to re-run.
      CANCELLED — pipeline was stopped.

    Non-terminal (call again):
      RUNNING / PENDING — still in progress; call wait_for_dataset again.
    """
    try:
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


@mcp.tool()
def retry_dataset(dataset_id: str) -> str:
    """Retry a failed or errored pipeline job.

    Call this when wait_for_dataset returns a FAILED or ERROR state.
    After retrying, call wait_for_dataset again to monitor the new run.
    """
    ok = get_client().datasets.retry(dataset_id)
    return "Dataset retry triggered successfully" if ok else "Failed to retry dataset"


@mcp.tool()
def delete_dataset(dataset_id: str) -> str:
    """Permanently delete a pipeline result dataset.

    Use this to remove unwanted or failed analysis results. This action
    cannot be undone. Do not delete the initial INTENSITY dataset unless
    you intend to re-process the upload from scratch.
    """
    ok = get_client().datasets.delete(dataset_id)
    return "Dataset deleted successfully" if ok else "Failed to delete dataset"
