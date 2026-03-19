import json
from typing import Any, Dict, List

from . import mcp
from .datasets import (
    delete_dataset,
    find_initial_dataset,
    list_datasets,
    retry_dataset,
    wait_for_dataset,
)
from .files import load_metadata_from_csv, plan_wide_to_md_format, read_csv_preview
from .health import health_check
from .pipelines import (
    describe_pipeline,
    generate_pairwise_comparisons,
    run_dose_response,
    run_normalisation_imputation,
    run_pairwise_comparison,
)
from .uploads import (
    create_upload,
    get_upload,
    update_sample_metadata,
    validate_upload_inputs,
    wait_for_upload,
)

_TOOL_REGISTRY: Dict[str, Any] = {
    "read_csv_preview": read_csv_preview,
    "load_metadata_from_csv": load_metadata_from_csv,
    "plan_wide_to_md_format": plan_wide_to_md_format,
    "health_check": health_check,
    "get_upload": get_upload,
    "create_upload": create_upload,
    "validate_upload_inputs": validate_upload_inputs,
    "update_sample_metadata": update_sample_metadata,
    "wait_for_upload": wait_for_upload,
    "list_datasets": list_datasets,
    "find_initial_dataset": find_initial_dataset,
    "wait_for_dataset": wait_for_dataset,
    "retry_dataset": retry_dataset,
    "delete_dataset": delete_dataset,
    "describe_pipeline": describe_pipeline,
    "run_normalisation_imputation": run_normalisation_imputation,
    "generate_pairwise_comparisons": generate_pairwise_comparisons,
    "run_pairwise_comparison": run_pairwise_comparison,
    "run_dose_response": run_dose_response,
}


@mcp.tool()
def batch(
    operations: List[Dict[str, Any]],
    stop_on_error: bool = True,
) -> str:
    """Execute multiple MCP tool calls in a single request.

    Collapses sequential tool calls into one round-trip — use this whenever you
    need to chain two or more operations (e.g. health_check + get_upload + list_datasets).

    operations: list of {"tool": "<name>", "params": {...}} objects executed in order.
    stop_on_error: stop on first failure (default true); set false to continue.

    Available tools: read_csv_preview, load_metadata_from_csv, plan_wide_to_md_format,
    health_check,
    get_upload, create_upload, validate_upload_inputs, update_sample_metadata,
    wait_for_upload, list_datasets, find_initial_dataset, wait_for_dataset,
    retry_dataset, delete_dataset, describe_pipeline, run_normalisation_imputation,
    generate_pairwise_comparisons, run_pairwise_comparison, run_dose_response.

    get_upload accepts either upload_id (UUID) or name (string). When only a name
    is known, use name — the tool resolves it to a UUID. Subsequent operations in
    the same batch can then use the UUID from the result.

    Example — full workflow from name only:
      operations=[
        {"tool": "get_upload", "params": {"name": "My Experiment"}},
        {"tool": "list_datasets", "params": {"upload_id": "<uuid-from-above>"}},
        {"tool": "find_initial_dataset", "params": {"upload_id": "<uuid-from-above>"}}
      ]

    Example — lookup by UUID:
      operations=[
        {"tool": "health_check"},
        {"tool": "get_upload", "params": {"upload_id": "<uuid>"}},
        {"tool": "list_datasets", "params": {"upload_id": "<uuid>"}}
      ]

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
            }
            results.append(entry)
            if stop_on_error:
                break
            continue

        try:
            output = fn(**params)
            results.append({"index": i, "tool": tool_name, "result": output})
        except Exception as e:
            results.append({"index": i, "tool": tool_name, "error": str(e)})
            if stop_on_error:
                break

    return json.dumps(results, indent=2)
