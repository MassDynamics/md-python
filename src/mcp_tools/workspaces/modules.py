"""Module CRUD MCP tools — placing dashboard modules on tabs.

The visualisation layer of the API. Every module:
  * lives on a tab's react-grid-layout grid (``x``, ``y``, ``width``,
    ``height`` in grid units; the canvas is 12 columns wide on the app),
  * declares its parameters via the registry (``describe_module_type``),
  * persists those parameters under ``settings`` in the layout.

The server does NOT merge registry-declared defaults at create time. To
guarantee the rendered widget always carries a complete settings hash,
``add_module_to_tab`` uses the client's ``create_with_defaults`` helper
under the hood — every default is sent on the wire even when the LLM
does not change it.
"""

import json
import time
from typing import Any, Dict, List, Optional

from md_python.resources.v2.workspaces import RenderVisualisationError

from .. import mcp
from .._client import get_client
from .._destructive import _attach_destructive
from . import _introspect
from ._mandates import _attach_visualisation
from ._modules_validation import _module_to_dict, resolve_dataset_settings

# Server-side rendering can take a long wall-clock time, but the LLM should
# not silently retry forever — the FastMCP instructions cap wait_* calls at
# ~10 polls before a check-in. render_module_visualisation enforces a
# matching internal cap so one MCP call costs at most this many HTTP
# round-trips, then returns a "still rendering" envelope to the LLM.
_RENDER_MAX_POLLS = 10


def _render_error_envelope(
    e: RenderVisualisationError, workspace_id: str, tab_id: str, module_id: str
) -> str:
    """Build a structured error envelope from a render failure.

    The vis-service returns a real error body on failure (e.g.
    ``{"error": "Visualisation not supported for module type '...'"}``).
    Surfacing it as a structured ``{"status": "error", ...}`` envelope —
    rather than a flat ``Error: ...`` string — lets the LLM branch on the
    failure: inform the user, decide whether to retry, or pick another
    module. ``http_status`` distinguishes a client error (4xx — usually a
    bad module/dataset or an unsupported module type, not worth retrying)
    from a server error (5xx — may be transient).
    """
    return json.dumps(
        {
            "status": "error",
            "http_status": e.status_code,
            "error": e.error,
            "workspace_id": workspace_id,
            "tab_id": tab_id,
            "module_id": module_id,
            "detail": e.body,
        },
        indent=2,
    )


@mcp.tool()
def add_module_to_tab(
    workspace_id: str,
    tab_id: str,
    item_id: str,
    x: int,
    y: int,
    width: int,
    height: int = 16,
    dataset_id: Optional[str] = None,
    dataset_ids: Optional[List[str]] = None,
    upload_id: Optional[str] = None,
    upload_ids: Optional[List[str]] = None,
    entity_type: Optional[str] = None,
    comparison: Optional[List[str]] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> str:
    """Place a dashboard module on a tab's grid.

    Wraps ``client.workspaces.modules.create_with_defaults`` — every
    registry-declared default for ``item_id`` is sent in the persisted
    settings hash, even keys the LLM does not change. Without this the
    rendered widget surfaces "Please provide Size / Color By / ..."
    prompts because the API does not merge defaults server-side.

    UPLOAD vs DATASET — KNOW THE DIFFERENCE.
    Mass Dynamics has two kinds of uuids and they are NOT
    interchangeable:

      * UPLOAD  — the parent record. Holds the sample metadata + the
                  raw / converted file pointers. One upload has many
                  datasets. ``upload_id`` is what you pass to
                  get_upload_sample_metadata, find_initial_dataset,
                  run_normalisation_imputation, etc.
      * DATASET — an analytical artefact under an upload. Has a
                  ``type`` (INTENSITY / PAIRWISE / DOSE_RESPONSE /
                  ANOVA). This is what plots VISUALISE. ``dataset_id``
                  is what you pass HERE (and to download_dataset_table,
                  query_entities, etc.).

    A common bug — and the one the rendered widget reports as
    "Table Proteingroup_Intensity not found for dataset <uuid>" — is
    passing an upload_id where a dataset_id is needed. This tool
    validates ``ds.type`` against the module's required dataset type
    and fails fast with an explicit "did you pass an upload_id?"
    message when they mismatch.

    DATASET BINDING (most plot modules need one). The dataset is
    encoded on the wire as a non-trivial envelope — not a bare id —
    that includes the dataset's name and its upload_id (passed as
    ``experimentId``). To remove the trap of hand-crafting that
    envelope, this tool takes the dataset as a first-class argument:

      * Single-dataset modules (parameters.multiple=False, e.g.
        ``missing_values_heatmap``, ``dimensionality_reduction_plot``,
        ``pairwise_volcano_plot``):
          dataset_id="<uuid>", upload_id="<uuid>"
      * Multi-dataset modules (parameters.multiple=True, e.g.
        ``dose_response_curve_plot``):
          dataset_ids=["<uuid>", ...], upload_ids=["<uuid>", ...]
        (same length, same order; one upload per dataset)
      * Dataset-free modules (heading, page_break, text):
          do NOT pass any of the above.

    The right shape for a given module is published in
    describe_module_type(item_id)["dataset_input"] — the LLM should
    always check that block before calling this tool. If the arity
    is wrong (e.g. dataset_id passed to a multiple=True module), this
    tool fails fast with a clear message before any API roundtrip.

    Args:
      workspace_id, tab_id: Where to place the module.
      item_id: The registry id (use list_module_types /
        describe_module_type to discover and inspect).
      x, y: Grid coordinates of the top-left corner. The canvas is 12
        columns wide; rows have no hard cap. (0, 0) is top-left.
      width, height: Size in grid units (NOT pixels). ``height``
        defaults to 16 — leave it unset unless the user asks for a
        specific size. A defaulted 16 reliably fits the render; smaller
        values tend to crop plots.
        * Heading / text / page_break: 12x1 typical (full-width, single
          row) — pass height=1 explicitly for these.
        * PLOT modules (volcano, heatmap, PCA, dose-response, box plot,
          all the Quality control plots, etc.): height MUST be at least
          16 (the default). Smaller heights crop the rendered
          visualisation and the legend / axis labels collapse on top of
          the data. Width is typically 6 (half-canvas, two side-by-side)
          or 12 (full-width).
        * Tables (dataset_table, list_table, qc_summary_table): height
          16+ as well; the table renders its own scrollbar but needs
          vertical room for the header + at least a few rows.
      dataset_id: Single-dataset modules — UUID of the dataset to
        visualise. Must be of the type required by
        describe_module_type[dataset_input][dataset_type]
        (INTENSITY for QC + experiment plots, PAIRWISE for volcano /
        heatmap, DOSE_RESPONSE for DR curves, ANOVA for ANOVA volcano).
        Mutually exclusive with dataset_ids.
      dataset_ids: Multi-dataset modules — list of UUIDs. Mutually
        exclusive with dataset_id.
      upload_id: Required companion to dataset_id. UUID of the upload
        that produced the dataset (Mass Dynamics persists this as
        experimentId in the envelope). Recover via the workflow that
        produced the dataset (find_initial_dataset(upload_id) gave you
        both, or list_datasets(upload_id)).
      upload_ids: Required companion to dataset_ids. Same length and
        order as dataset_ids — one upload_id per dataset.
      entity_type: One of ``"protein"``, ``"peptide"``, ``"gene"``,
        ``"metabolite"``. Required for any module that has an EntityType
        field — that is, almost every plot module. The dataset payload
        does NOT carry the entity type, so the LLM MUST supply it:
          * md_format / DIA-NN / MaxQuant / Spectronaut uploads ->
            "protein" or "peptide"
          * md_format_gene uploads -> "gene"
          * md_format_metabolite uploads -> "metabolite"
        Confirm with the user when uncertain. Modules without an
        EntityType field (heading, page_break, text) reject this arg.
        vis-service is the final arbiter — a module that does not yet
        support metabolite will reject it at render time.
      comparison: PAIRWISE modules only (e.g. ``pairwise_volcano_plot``).
        The ``[left, right]`` case/control pair to plot, as two condition
        names — positive log2FC means ``left`` is more abundant than
        ``right``. The two names must match (in either order) one of the
        comparisons the dataset was actually run with
        (``job_run_params.condition_comparisons``; inspect via
        get_dataset). When omitted, this tool auto-fills the FIRST
        comparison the dataset carries, oriented case-vs-control — so the
        volcano always renders with a real comparison shape rather than an
        empty default. Requires dataset_id to resolve; rejected for
        modules with no ConditionComparison field.
      settings: Per-parameter values for everything OTHER than the
        dataset binding and entity_type. Keys override registry
        defaults; the dataset envelope and entity_type this tool
        builds are merged on top last so a malformed user value
        cannot accidentally override the structured fields.

    Returns prose: ``Module placed. ID: <uuid>\\n<module JSON>``

    Raises (returned as ``Error: <message>`` prose):
      * Unknown item_id (not in registry / not available to user).
      * Required key without a registry default that the caller did not
        supply — fail-fast happens client-side before the request leaves.
      * Dataset-arity mismatch (multiple=False vs dataset_ids passed,
        or vice versa).
      * Missing upload_id / upload_ids companion.
      * upload_ids length mismatched with dataset_ids.
      * Dataset args passed to a module that does not accept a dataset.
      * 400 from the server when settings keys are not in the module's
        declared input_settings.

    See also: describe_module_type, update_tab_module.
    """
    client = get_client()
    try:
        structured_overlay = resolve_dataset_settings(
            client=client,
            item_id=item_id,
            dataset_id=dataset_id,
            dataset_ids=dataset_ids,
            upload_id=upload_id,
            upload_ids=upload_ids,
            entity_type=entity_type,
            comparison=comparison,
        )
    except ValueError as e:
        return f"Error: {e}"

    # Field-type-level fallbacks for required-no-registry-default fields
    # whose JS instruction layer in the workflow repo picks a concrete
    # shape (e.g. xAxis on cv_distribution_plot defaults to
    # [{field:'sample_name', order:'none'}] in the
    # OrderableSampleMetadataColumns instruction). Layered BELOW user
    # settings so the LLM can still override.
    module = client.module_registry.get(item_id)
    fallbacks: Dict[str, Any] = {}
    if module is not None:
        fallbacks = _introspect.field_type_fallbacks(module)

    # Merge order: fallbacks → user settings → structured overlay
    # (dataset envelope + entity_type). The structured overlay wins so
    # the LLM cannot accidentally override the structured fields by
    # passing settings={"datasetsSearch": "..."} or
    # settings={"entityType": "..."}. create_with_defaults still adds
    # registry defaults below this.
    merged_settings: Dict[str, Any] = {}
    merged_settings.update(fallbacks)
    if settings:
        merged_settings.update(settings)
    merged_settings.update(structured_overlay)

    try:
        mod = client.workspaces.modules.create_with_defaults(
            workspace_id=workspace_id,
            tab_id=tab_id,
            item_id=item_id,
            x=x,
            y=y,
            width=width,
            height=height,
            settings=merged_settings,
        )
    except ValueError as e:
        return f"Error: {e}"
    return (
        f"Module placed. ID: {mod.id}\n" f"{json.dumps(_module_to_dict(mod), indent=2)}"
    )


@mcp.tool()
def list_tab_modules(workspace_id: str, tab_id: str) -> str:
    """List every module currently placed on a tab.

    No pagination — modules per tab are typically small (single digits to
    low double digits). Returns JSON: ``{"data": [TabModule, ...]}``.
    """
    mods = get_client().workspaces.modules.list(workspace_id, tab_id)
    return json.dumps({"data": [_module_to_dict(m) for m in mods]}, indent=2)


@mcp.tool()
def get_tab_module(workspace_id: str, tab_id: str, module_id: str) -> str:
    """Fetch a single module by id (scoped to its tab)."""
    mod = get_client().workspaces.modules.get(workspace_id, tab_id, module_id)
    if mod is None:
        return json.dumps({"error": f"Module {module_id!r} not found"}, indent=2)
    return json.dumps(_module_to_dict(mod), indent=2)


@mcp.tool()
def update_tab_module(
    workspace_id: str,
    tab_id: str,
    module_id: str,
    item_id: str,
    x: Optional[int] = None,
    y: Optional[int] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> str:
    """Move / resize / re-configure an already-placed module.

    Two important contracts to respect:

    1. ``item_id`` is REQUIRED on every PUT — server-side bug: the update
       endpoint reads ``existing['item_id']`` from the persisted hash but
       persistence stores it under camelCase ``itemId``. A partial update
       without item_id always fails with "item_id can't be blank". Re-send
       the original item_id even if you are only nudging x/y/w/h.

    2. ``settings`` is REPLACED wholesale on PUT — there is no per-key
       merge. If you only want to change one key, you MUST rebuild the full
       hash (registry defaults + user values + your change) and pass that
       in. The cleanest pattern:

         old = get_tab_module(workspace_id, tab_id, module_id)
         registry_defaults = describe_module_type(old.item_id).registry_defaults
         new_settings = {**registry_defaults, **old.settings, "key": new_value}
         update_tab_module(..., settings=new_settings)

       Otherwise the rendered widget will revert to "Please provide ..."
       prompts for any key you accidentally drop.

    Args:
      item_id: REQUIRED — the original module's item_id (see contract 1).
      x, y, width, height: New grid coordinates (omit to keep current).
        Note: plot modules need height >= 16 to render properly; smaller
        heights crop the visualisation. See add_module_to_tab for the
        full sizing guidance.
      settings: Full settings hash (see contract 2).

    Returns the updated module JSON.
    """
    mod = get_client().workspaces.modules.update(
        workspace_id,
        tab_id,
        module_id,
        item_id=item_id,
        x=x,
        y=y,
        width=width,
        height=height,
        settings=settings,
    )
    return json.dumps(_module_to_dict(mod), indent=2)


@mcp.tool()
def remove_module_from_tab(
    workspace_id: str,
    tab_id: str,
    module_id: str,
) -> str:
    """Remove a module from a tab's layout.

    Returns prose: ``Module removed successfully. ID: <uuid>``
    """
    get_client().workspaces.modules.delete(workspace_id, tab_id, module_id)
    return f"Module removed successfully. ID: {module_id}"


@mcp.tool()
def add_text_module(
    workspace_id: str,
    tab_id: str,
    text: str,
    x: int = 0,
    y: int = 0,
    width: int = 12,
    height: int = 3,
) -> str:
    """Add a text module to a tab in a single call.

    The text module is a content block — narrative, headings, embedded
    images — distinct from a visualisation. Its body lives at
    ``settings.text`` and accepts HTML, including base64-embedded
    ``<img>`` tags. There is NO separate "set content" step: the body
    is sent in the same POST as the layout fields.

    Unlike :func:`add_module_to_tab`, the visualisation Q&A mandate
    does NOT apply here — there is only one user-supplied value (the
    body) and no statistical parameters to walk through.

    Sizing defaults to a full-width 12x3 block, which is the common
    "section divider with a paragraph" shape on a typical dashboard.
    Pass explicit ``x``, ``y``, ``width``, ``height`` to place it
    differently. Server-side validation will reject ``settings.text``
    longer than the registry's ``parameters.maxLength``; the 4xx is
    surfaced verbatim.

    Returns the created module JSON.
    """
    try:
        mod = get_client().workspaces.modules.create_text(
            workspace_id=workspace_id,
            tab_id=tab_id,
            text=text,
            x=x,
            y=y,
            width=width,
            height=height,
        )
    except (ValueError, Exception) as e:
        return f"Error: {e}"
    return (
        f"Text module placed. ID: {mod.id}\n"
        f"{json.dumps(_module_to_dict(mod), indent=2)}"
    )


@mcp.tool()
def update_text_module(
    workspace_id: str,
    tab_id: str,
    module_id: str,
    text: str,
) -> str:
    """Replace the body of an existing text module.

    Sends only ``{"settings": {"text": text}}``; the layout keys
    (x/y/width/height) are NOT included so the server preserves them
    verbatim. Use :func:`update_tab_module` if you also need to move
    or resize the module.

    Server-side validation will reject ``settings.text`` longer than
    the registry's ``parameters.maxLength``; the 4xx is surfaced
    verbatim.

    Returns the updated module JSON.
    """
    try:
        mod = get_client().workspaces.modules.update_text(
            workspace_id=workspace_id,
            tab_id=tab_id,
            module_id=module_id,
            text=text,
        )
    except (ValueError, Exception) as e:
        return f"Error: {e}"
    return json.dumps(_module_to_dict(mod), indent=2)


@mcp.tool()
def render_module_visualisation(
    workspace_id: str,
    tab_id: str,
    module_id: str,
    poll: bool = True,
    timeout_s: float = 300.0,
) -> str:
    """Fetch the rendered visualisation JSON for an existing module.

    Calls ``GET /workspaces/:ws/tabs/:tab/modules/:id/visualisation``.
    The server returns 200 with the visualisation payload when it is
    ready, or 202 while still rendering (with a ``Retry-After`` header).

    Args:
      workspace_id, tab_id, module_id: Identify the module to render.
      poll: When True (default), the tool follows 202 → wait → re-request
        up to a bounded number of times (``_RENDER_MAX_POLLS``, currently
        10). When the cap is hit while the server is still 202, the tool
        returns a ``{"status": "rendering", "polls": N, "retry_after":
        int}`` envelope so the LLM can decide whether to call this tool
        again (treating the next call as one more "wait") or check in
        with the user. When ``poll`` is False, the tool returns the same
        envelope after a single HTTP request.
      timeout_s: Soft total wall-clock cap (default 300s). The hard cap
        is ``_RENDER_MAX_POLLS`` HTTP requests — whichever fires first
        produces the "still rendering" envelope.

    Use this AFTER add_module_to_tab when you want the rendered payload
    out-of-band (e.g. to embed in a report or downstream tool). The
    workspace UI fetches the same endpoint automatically when a user
    opens the tab — calling this tool is not required for the module to
    appear in the app.

    POLLING DISCIPLINE — one tool call = at most ``_RENDER_MAX_POLLS``
    server requests. If the envelope says "still rendering" after that,
    treat each subsequent call as one more "wait" toward the 10-poll
    check-in limit documented in the FastMCP instructions. After ~10
    "still rendering" envelopes in a row, report progress to the user
    and ask whether to keep waiting.

    Returns one of:
      * On success — the visualisation body as JSON.
      * Still rendering after the internal poll cap — a ``{"status":
        "rendering", ...}`` envelope.
      * Render FAILED (vis-service returned a non-200/202) — a structured
        error envelope::

            {
              "status": "error",
              "http_status": 400,
              "error": "<vis-service message>",
              "workspace_id": "...", "tab_id": "...", "module_id": "...",
              "detail": <parsed error body or raw text>
            }

        ACT on this envelope — do not silently swallow it. Tell the user
        the render failed and why (quote ``error``). ``http_status`` in
        the 4xx range is a client-side problem — an unsupported module
        type, a bad/!missing module or dataset — and retrying will not
        help; surface it and, if appropriate, suggest an alternative
        module or check the inputs. A 5xx may be transient — offer to
        retry. When the module type itself is unsupported by the render
        endpoint, the module still renders in the workspace UI; tell the
        user they can open the workspace to see it.
      * Other errors / timeouts — an ``Error: ...`` prose string.
    """
    # Track polls in the MCP layer so the LLM gets a hard guarantee that
    # one tool call never makes more than _RENDER_MAX_POLLS HTTP requests
    # regardless of what timeout_s says. The resource's own poll loop is
    # bypassed (poll=False) so we own the counting here.
    if not poll:
        try:
            body = get_client().workspaces.modules.render_visualisation(
                workspace_id=workspace_id,
                tab_id=tab_id,
                module_id=module_id,
                poll=False,
                timeout_s=timeout_s,
            )
        except RenderVisualisationError as e:
            return _render_error_envelope(e, workspace_id, tab_id, module_id)
        except Exception as e:
            return f"Error: {e}"
        return json.dumps(body, indent=2)

    client = get_client()
    deadline = time.monotonic() + max(0.0, timeout_s)
    last_retry_after = 0

    for poll_idx in range(_RENDER_MAX_POLLS):
        try:
            body = client.workspaces.modules.render_visualisation(
                workspace_id=workspace_id,
                tab_id=tab_id,
                module_id=module_id,
                poll=False,
                timeout_s=timeout_s,
            )
        except RenderVisualisationError as e:
            return _render_error_envelope(e, workspace_id, tab_id, module_id)
        except Exception as e:
            return f"Error: {e}"

        # Resource returns {"status": "rendering", "retry_after": int}
        # while the server is still 202; anything else is the rendered
        # visualisation body. Branch on the rendering sentinel.
        if (
            isinstance(body, dict)
            and body.get("status") == "rendering"
            and "retry_after" in body
        ):
            last_retry_after = int(body["retry_after"])
            # If we are about to exceed the timeout, surface the envelope
            # rather than sleeping past the deadline.
            if time.monotonic() + last_retry_after > deadline:
                return json.dumps(
                    {
                        "status": "rendering",
                        "polls": poll_idx + 1,
                        "retry_after": last_retry_after,
                        "reason": "timeout_s exceeded — call again to keep waiting",
                    },
                    indent=2,
                )
            time.sleep(last_retry_after)
            continue
        return json.dumps(body, indent=2)

    # Hit the internal poll cap without a 200. Hand off to the LLM.
    return json.dumps(
        {
            "status": "rendering",
            "polls": _RENDER_MAX_POLLS,
            "retry_after": last_retry_after,
            "reason": (
                f"reached internal poll cap ({_RENDER_MAX_POLLS} HTTP "
                "requests). Call render_module_visualisation again to "
                "keep waiting, or check in with the user."
            ),
        },
        indent=2,
    )


@mcp.tool()
def add_plotly_json_module(
    workspace_id: str,
    tab_id: str,
    plotly_json: Dict[str, Any],
    title: str = "",
    x: int = 0,
    y: int = 0,
    width: int = 12,
    height: int = 6,
) -> str:
    """Add a Plotly-JSON renderer module to a tab.

    The ``plotly_json_renderer`` module renders an arbitrary Plotly
    figure (``{"data": [...], "layout": {...}}``) client-side. Nothing
    is sent to the visualisations service — the figure JSON is stored
    verbatim in ``settings.plotlyJson`` and shipped to the browser.

    LOCAL-ONLY CAVEAT
    This module is gated by the ``plotly_json_renderer_module`` feature
    flag, which is currently enabled only on a local workflow app build
    (testing scaffold). On ``dev.massdynamics.com`` (and production) the
    flag is off, the module is hidden from the picker, and creating it
    via this tool will either fail server-side or render as an unknown
    module. Use only when you know the target environment has the flag
    enabled.

    Args:
      workspace_id: Parent workspace UUID.
      tab_id: Target tab UUID.
      plotly_json: A Plotly figure spec — at minimum a dict with ``data``
        (list of traces) and ``layout`` (dict). Passed through unchanged.
      title: Optional title used both as the plot name and as the
        download filename. Defaults to an empty string.
      x, y, width, height: Grid placement on the tab layout. Defaults
        sized for a single chart (12 wide × 6 tall).

    Skips the visualisation Q&A mandate: there is no platform-default
    parameter set to walk through — the user supplies the entire figure
    JSON.

    Returns the created module as JSON, or ``Error: ...`` on failure.
    """
    if not isinstance(plotly_json, dict):
        return (
            "Error: plotly_json must be a dict with `data` and `layout` "
            f"keys (got {type(plotly_json).__name__})"
        )

    settings: Dict[str, Any] = {"plotlyJson": plotly_json}
    if title:
        settings["title"] = title

    try:
        mod = get_client().workspaces.modules.create(
            workspace_id=workspace_id,
            tab_id=tab_id,
            item_id="plotly_json_renderer",
            x=x,
            y=y,
            width=width,
            height=height,
            settings=settings,
        )
    except (ValueError, Exception) as e:
        return f"Error: {e}"
    return (
        f"Plotly JSON module placed. ID: {mod.id}\n"
        f"{json.dumps(_module_to_dict(mod), indent=2)}"
    )


# Behavioural mandates — visualisation Q&A on add+update; destructive on remove.
# Note: add_text_module / update_text_module / add_plotly_json_module
# deliberately bypass the visualisation mandate — the body / figure is
# the user's own content, not a parameter that needs the platform-
# default vs LLM-recommendation table.
_attach_visualisation(add_module_to_tab, update_tab_module)
_attach_destructive(remove_module_from_tab)
