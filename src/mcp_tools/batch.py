import json
from typing import Any, Callable, Dict, List

from . import mcp

# Side-effect imports — bringing these modules in registers every @mcp.tool()
# decorator with the shared FastMCP instance. Order does not matter beyond
# "everything must be imported before _TOOL_REGISTRY is read".
from . import datasets as _datasets_pkg  # noqa: F401
from . import entities as _entities_mod  # noqa: F401
from . import entity_meta as _entity_meta_mod  # noqa: F401
from . import evosep as _evosep_mod  # noqa: F401
from . import files as _files_pkg  # noqa: F401
from . import health as _health_mod  # noqa: F401
from . import pipelines as _pipelines_pkg  # noqa: F401
from . import uploads as _uploads_pkg  # noqa: F401
from . import workspaces as _workspaces_pkg  # noqa: F401


# ──────────────────────────────────────────────────────────────────────────────
# Single source of truth for the batch dispatch table.
#
# Previously this module hand-maintained ``_TOOL_REGISTRY: Dict[str, fn]``
# alongside every ``@mcp.tool()`` declaration in the codebase. The two drifted
# every time a new tool was added — adding ``list_entity_lists``,
# ``render_module_visualisation``, etc. required two edits in two places.
#
# Now ``_TOOL_REGISTRY`` is derived from FastMCP's own tool manager. Every
# ``@mcp.tool()`` decorator registers the function with ``mcp._tool_manager``;
# we ask the manager for its tools and build a name→callable map from that.
# ``batch`` itself is excluded so the dispatch is non-recursive.
#
# A regression test (tests/mcp_tools/test_tool_registry_introspection.py)
# fails the moment a new @mcp.tool is declared but not surfaced here.
# ──────────────────────────────────────────────────────────────────────────────


def _build_tool_registry() -> Dict[str, Callable[..., Any]]:
    """Build the batch dispatch table from FastMCP introspection.

    Excludes ``batch`` itself to keep dispatch non-recursive. ``batch`` is
    listed in ``mcp._tool_manager`` because it carries ``@mcp.tool()`` for the
    LLM-facing surface, but inside the dispatch loop it must not be callable
    or the LLM could nest ``batch`` calls indefinitely.
    """
    registry: Dict[str, Callable[..., Any]] = {}
    for tool in mcp._tool_manager.list_tools():
        if tool.name == "batch":
            continue
        registry[tool.name] = tool.fn
    return registry


_TOOL_REGISTRY: Dict[str, Callable[..., Any]] = _build_tool_registry()


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

    Available tools: every @mcp.tool registered on this server EXCEPT ``batch``
    itself. See the TOOL CATEGORIES section in the FastMCP ``instructions``
    string (mcp_tools.__init__) for the canonical, grouped enumeration, or
    call ``get_workflow_guide`` for a one-line summary per tool. Passing an
    unknown tool name returns an ``error_code='unknown_tool'`` entry whose
    ``error`` field includes the full available-tools list.

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
      4. run_normalisation_imputation (valid methods: normalisation="median"/"quantile", imputation="mnar"/"knn")
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
