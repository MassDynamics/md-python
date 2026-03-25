import json
from typing import Any, Dict, List

from . import mcp
from .datasets import (
    delete_dataset,
    find_initial_dataset,
    find_initial_datasets,
    list_datasets,
    list_jobs,
    retry_dataset,
    wait_for_dataset,
)
from .files import load_metadata_from_csv, plan_wide_to_md_format, read_csv_preview
from .health import get_workflow_guide, health_check
from .pipelines import (
    describe_pipeline,
    generate_pairwise_comparisons,
    run_dose_response,
    run_dose_response_bulk,
    run_dose_response_from_upload,
    run_normalisation_imputation,
    run_pairwise_comparison,
)
from .uploads import (
    create_upload,
    create_upload_from_csv,
    get_upload,
    list_uploads_status,
    update_sample_metadata,
    validate_upload_inputs,
    wait_for_upload,
)

_TOOL_REGISTRY: Dict[str, Any] = {
    "read_csv_preview": read_csv_preview,
    "load_metadata_from_csv": load_metadata_from_csv,
    "plan_wide_to_md_format": plan_wide_to_md_format,
    "health_check": health_check,
    "get_workflow_guide": get_workflow_guide,
    "get_upload": get_upload,
    "create_upload": create_upload,
    "create_upload_from_csv": create_upload_from_csv,
    "list_uploads_status": list_uploads_status,
    "validate_upload_inputs": validate_upload_inputs,
    "update_sample_metadata": update_sample_metadata,
    "wait_for_upload": wait_for_upload,
    "list_jobs": list_jobs,
    "list_datasets": list_datasets,
    "find_initial_dataset": find_initial_dataset,
    "find_initial_datasets": find_initial_datasets,
    "wait_for_dataset": wait_for_dataset,
    "retry_dataset": retry_dataset,
    "delete_dataset": delete_dataset,
    "describe_pipeline": describe_pipeline,
    "run_normalisation_imputation": run_normalisation_imputation,
    "generate_pairwise_comparisons": generate_pairwise_comparisons,
    "run_pairwise_comparison": run_pairwise_comparison,
    "run_dose_response": run_dose_response,
    "run_dose_response_from_upload": run_dose_response_from_upload,
    "run_dose_response_bulk": run_dose_response_bulk,
}


@mcp.tool()
def batch(
    operations: List[Dict[str, Any]],
    stop_on_error: bool = True,
) -> str:
    """Execute multiple MCP tool calls in a single request.

    Collapses sequential tool calls into one round-trip. Use this to chain
    independent or sequential operations without waiting for individual responses.
    Always keep stop_on_error=True (default) for pipeline workflows — if any step
    fails, stopping immediately prevents passing a broken dataset ID to the next step.

    operations: list of {"tool": "<name>", "params": {...}} objects executed in order.
    stop_on_error: stop on first failure (default true); set false only for
      independent inspection operations.

    Available tools: read_csv_preview, load_metadata_from_csv, plan_wide_to_md_format,
    health_check, get_workflow_guide,
    get_upload, create_upload, validate_upload_inputs, update_sample_metadata,
    wait_for_upload, list_jobs, list_datasets,
    find_initial_dataset, find_initial_datasets, wait_for_dataset,
    retry_dataset, delete_dataset, describe_pipeline, run_normalisation_imputation,
    generate_pairwise_comparisons, run_pairwise_comparison,
    run_dose_response, run_dose_response_from_upload, run_dose_response_bulk.

    ── WORKFLOW EXAMPLE A: inspect an upload by name ────────────────────────────
      operations=[
        {"tool": "get_upload", "params": {"name": "My Experiment"}},
        {"tool": "list_datasets", "params": {"upload_id": "<uuid-from-above>"}},
        {"tool": "find_initial_dataset", "params": {"upload_id": "<uuid-from-above>"}}
      ]

    ── WORKFLOW EXAMPLE B: prepare and validate metadata before upload ───────────
    (Collapse metadata loading + validation into one round-trip)
      operations=[
        {"tool": "load_metadata_from_csv", "params": {"file_path": "/path/to/metadata.csv"}},
        {"tool": "validate_upload_inputs", "params": {
            "experiment_design": "<experiment_design-from-above>",
            "sample_metadata": "<sample_metadata-from-above>"
        }}
      ]

    ── WORKFLOW EXAMPLE C: look up pipeline schemas when needed ─────────────────
    (Only needed if you need to verify valid parameter values — not required for known params)
      operations=[
        {"tool": "describe_pipeline", "params": {"job_slug": "normalisation_imputation"}},
        {"tool": "describe_pipeline", "params": {"job_slug": "pairwise_comparison"}}
      ]

    ── FULL DEA WORKFLOW (run as separate calls, not one batch) ─────────────────
    The full differential expression analysis (DEA) workflow must be broken into
    phases because wait_for_upload and wait_for_dataset are long-running blocking
    calls — batch them separately from each phase:

    Phase 1 — prepare and upload:
      1. load_metadata_from_csv → validate_upload_inputs → create_upload
      2. wait_for_upload (separate call — may take minutes)

    Phase 2 — normalisation:
      3. find_initial_dataset
      4. run_normalisation_imputation (valid methods: normalisation="median"/"quantile", imputation="min_value"/"knn")
      5. wait_for_dataset (separate call)

    Phase 3 — pairwise comparison:
      6. generate_pairwise_comparisons → run_pairwise_comparison
      7. wait_for_dataset (separate call)
      (call describe_pipeline("pairwise_comparison") first if you need to verify valid parameter values)

    ── FULL DRA WORKFLOW (dose-response analysis) ───────────────────────────────
    Same as DEA Phases 1-2, then:

    Phase 3 — dose-response (requires ≥3 distinct doses, ≥5 replicates):
      6. run_dose_response_from_upload (single job) or run_dose_response_bulk (many jobs)
      7. wait_for_dataset (separate call)
      (call describe_pipeline("dose_response") first if you need to verify valid parameter values)

    Returns JSON array with each operation's index, tool name, and result or error.
    """
    results = []
    for i, op in enumerate(operations):
        tool_name = op.get("tool", "")
        params = op.get("params", {}) or {}

        fn = _TOOL_REGISTRY.get(tool_name)
        if not fn:
            entry = {
                "index": i,
                "tool": tool_name,
                "error": f"Unknown tool '{tool_name}'. Available: {sorted(_TOOL_REGISTRY)}",
                "error_code": "unknown_tool",
            }
            results.append(entry)
            if stop_on_error:
                break
            continue

        try:
            output = fn(**params)
            results.append({"index": i, "tool": tool_name, "result": output})
        except Exception as e:
            results.append(
                {
                    "index": i,
                    "tool": tool_name,
                    "error": str(e),
                    "error_code": "exception",
                }
            )
            if stop_on_error:
                break

    return json.dumps(results, indent=2)
