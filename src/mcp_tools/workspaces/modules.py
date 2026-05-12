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
from typing import Any, Dict, List, Optional

from md_python.models import TabModule

from .. import mcp
from .._client import get_client
from .._destructive import _attach_destructive
from . import _introspect
from ._mandates import _attach_visualisation


def _module_to_dict(m: TabModule) -> Dict[str, Any]:
    return {
        "id": str(m.id),
        "item_id": m.item_id,
        "x": m.x,
        "y": m.y,
        "width": m.width,
        "height": m.height,
        "settings": m.settings,
    }


_VALID_ENTITY_TYPES = ("protein", "peptide", "gene")


def _check_dataset_type(
    ds_id: str, ds_type: Optional[str], required: Optional[str]
) -> None:
    """Hard-fail when the persisted dataset's ``type`` doesn't match the
    module's required dataset_type.

    The most common cause is the LLM confusing **upload_id** (parent
    record holding sample metadata) with **dataset_id** (the actual
    analytical artefact — INTENSITY / PAIRWISE / DOSE_RESPONSE / ANOVA).
    We surface that hypothesis explicitly in the error message.
    """
    if required is None or ds_type is None or ds_type == required:
        return
    raise ValueError(
        f"dataset_id {ds_id!r} is type {ds_type!r}, but the module "
        f"requires a {required!r} dataset.\n\n"
        "Common causes:\n"
        "  * You passed an UPLOAD id where a DATASET id is needed. "
        "Uploads are parent records (they hold sample metadata + raw "
        "files). Datasets are the analytical artefacts placed on plots "
        "(INTENSITY for QC + experiment, PAIRWISE for volcano / "
        "heatmap, DOSE_RESPONSE for DR curves, ANOVA for ANOVA "
        "volcano). One upload has many datasets.\n"
        "  * You passed an INTENSITY dataset to a module that needs a "
        "downstream output. For PAIRWISE you must run "
        "run_pairwise_comparison first; for ANOVA, run_anova; for "
        "DOSE_RESPONSE, run_dose_response.\n"
        "  * You passed an old / wrong dataset id. Use list_datasets / "
        "find_initial_dataset / query_datasets to look up the right id."
    )


def _resolve_entity_type_settings(
    item_id: str,
    entity_type: Optional[str],
    module: Any,
) -> Dict[str, Any]:
    """Validate entity_type against the module's spec and return
    ``{settings_key: entity_type}`` for merging into settings.

    Returns an empty dict when the module has no EntityType field.
    Fails-fast when the field is required and the LLM did not supply a
    value, or when the supplied value is not in {protein, peptide, gene}.
    """
    eti = _introspect.entity_type_input_for(module)
    if eti is None:
        if entity_type is not None:
            raise ValueError(
                f"module {item_id!r} does not accept entity_type (no "
                "EntityType-typed parameter in its registry spec); drop "
                "entity_type"
            )
        return {}
    if entity_type is None:
        if eti["required"]:
            raise ValueError(
                f"module {item_id!r} requires entity_type — one of "
                f"{eti['valid_values']}. The dataset payload does NOT "
                "carry the entity type, so the LLM must supply it: "
                "protein/peptide for md_format / DIA-NN / MaxQuant / "
                "Spectronaut uploads, gene for md_format_gene uploads. "
                "Confirm with the user when uncertain."
            )
        return {}
    if entity_type not in eti["valid_values"]:
        raise ValueError(
            f"entity_type must be one of {eti['valid_values']}, got " f"{entity_type!r}"
        )
    return {eti["settings_key"]: entity_type}


def _resolve_dataset_settings(
    item_id: str,
    dataset_id: Optional[str],
    dataset_ids: Optional[List[str]],
    upload_id: Optional[str],
    upload_ids: Optional[List[str]],
    entity_type: Optional[str],
) -> Dict[str, Any]:
    """Validate dataset + entity_type args against the module's spec and
    return a settings overlay ready to merge into ``settings``.

    Raises ``ValueError`` with a clear message on every shape mismatch:
      * arity mismatch (single vs multiple) vs the registry's
        ``parameters.multiple`` flag,
      * dataset.type mismatch vs ``parameters.type`` (the most common
        symptom of the LLM confusing upload_id with dataset_id),
      * missing companion upload_id(s),
      * companion list-length mismatch,
      * dataset args passed to a module that does NOT have a Datasets
        field (e.g. ``heading``, ``page_break``),
      * entity_type missing for a module that requires it,
      * entity_type passed for a module that does not accept one.
    """
    has_id_arg = dataset_id is not None
    has_ids_arg = dataset_ids is not None
    has_any = has_id_arg or has_ids_arg

    client = get_client()
    module = client.module_registry.get(item_id)
    if module is None:
        raise ValueError(
            f"item_id {item_id!r} is not in the module registry "
            "(or is not available to the current user)"
        )

    overlay: Dict[str, Any] = {}
    overlay.update(_resolve_entity_type_settings(item_id, entity_type, module))

    di = _introspect.dataset_input_for(module)

    # Module has no Datasets field (heading, page_break, text, …).
    if di is None:
        if has_any:
            raise ValueError(
                f"module {item_id!r} does not accept a dataset (no "
                "Datasets-typed parameter in its registry spec); drop "
                "dataset_id / dataset_ids"
            )
        return overlay

    # Module has a Datasets field. If the LLM didn't pass anything and
    # the field is required, fail-fast.
    if not has_any:
        if di["required"]:
            raise ValueError(
                f"module {item_id!r} requires a dataset (settings_key="
                f"{di['settings_key']!r}, arity={di['arity']!r}, "
                f"dataset_type={di['dataset_type']!r}). Pass "
                f"{di['tool_args']['ids']}=... and "
                f"{di['tool_args']['uploads']}=...\n\n"
                "REMEMBER: dataset_id is the DATASET (analytical "
                "artefact), upload_id is the UPLOAD (parent record). "
                "They are different uuids."
            )
        return overlay

    # XOR check.
    if has_id_arg and has_ids_arg:
        raise ValueError(
            "pass dataset_id OR dataset_ids, not both — "
            f"module {item_id!r} has arity {di['arity']!r}"
        )

    # Arity check.
    if di["arity"] == "single" and has_ids_arg:
        raise ValueError(
            f"module {item_id!r} has arity 'single' (parameters.multiple"
            "=False); pass dataset_id, not dataset_ids"
        )
    if di["arity"] == "multiple" and has_id_arg:
        raise ValueError(
            f"module {item_id!r} has arity 'multiple' (parameters.multiple"
            "=True); pass dataset_ids, not dataset_id"
        )

    # Build the envelope.
    if di["arity"] == "single":
        if upload_id is None:
            raise ValueError(
                "dataset_id requires upload_id — passed as experimentId in "
                "the persisted envelope. Use find_initial_dataset / "
                "list_datasets to recover the upload_id paired with the "
                "dataset_id. (upload_id is the PARENT upload's uuid; "
                "dataset_id is the DATASET's uuid — they are different.)"
            )
        assert dataset_id is not None  # narrowed by has_id_arg + arity branch
        ds = client.datasets.get_by_id(dataset_id)
        if ds is None:
            raise ValueError(
                f"dataset_id {dataset_id!r} not found (or no permission). "
                "Did you pass an upload_id by mistake? Run list_datasets / "
                "find_initial_dataset to look up the right dataset uuid."
            )
        _check_dataset_type(str(ds.id), ds.type, di["dataset_type"])
        envelope = _introspect.build_dataset_envelope(
            dataset_id=str(ds.id),
            dataset_name=ds.name,
            upload_id=upload_id,
            dataset_type=di["dataset_type"],
        )
        overlay[di["settings_key"]] = envelope
        return overlay

    # arity == "multiple"
    assert dataset_ids is not None  # for mypy
    if upload_ids is None:
        raise ValueError(
            "dataset_ids requires upload_ids — one per dataset, same "
            "order. Each upload_id is persisted as experimentId in the "
            "envelope's individualResults."
        )
    if len(dataset_ids) != len(upload_ids):
        raise ValueError(
            f"dataset_ids has {len(dataset_ids)} entries but upload_ids "
            f"has {len(upload_ids)} — they must match length and order"
        )
    if not dataset_ids:
        raise ValueError("dataset_ids cannot be empty")

    entries: List[Dict[str, str]] = []
    for did, uid in zip(dataset_ids, upload_ids):
        ds = client.datasets.get_by_id(did)
        if ds is None:
            raise ValueError(
                f"dataset_id {did!r} not found (or no permission). "
                "Did you pass an upload_id by mistake?"
            )
        _check_dataset_type(str(ds.id), ds.type, di["dataset_type"])
        entries.append({"id": str(ds.id), "name": ds.name, "upload_id": uid})
    envelope = _introspect.build_dataset_envelope_multi(
        entries, dataset_type=di["dataset_type"]
    )
    overlay[di["settings_key"]] = envelope
    return overlay


@mcp.tool()
def add_module_to_tab(
    workspace_id: str,
    tab_id: str,
    item_id: str,
    x: int,
    y: int,
    width: int,
    height: int,
    dataset_id: Optional[str] = None,
    dataset_ids: Optional[List[str]] = None,
    upload_id: Optional[str] = None,
    upload_ids: Optional[List[str]] = None,
    entity_type: Optional[str] = None,
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
      width, height: Size in grid units (NOT pixels).
        * Heading / text / page_break: 12x1 typical (full-width, single
          row).
        * PLOT modules (volcano, heatmap, PCA, dose-response, box plot,
          all the Quality control plots, etc.): height MUST be at least
          12. Smaller heights crop the rendered visualisation and the
          legend / axis labels collapse on top of the data. Width is
          typically 6 (half-canvas, two side-by-side) or 12 (full-width).
        * Tables (dataset_table, list_table, qc_summary_table): height
          12+ as well; the table renders its own scrollbar but needs
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
      entity_type: One of ``"protein"``, ``"peptide"``, ``"gene"``.
        Required for any module that has an EntityType field — that
        is, almost every plot module. The dataset payload does NOT
        carry the entity type, so the LLM MUST supply it:
          * md_format / DIA-NN / MaxQuant / Spectronaut uploads ->
            "protein" or "peptide"
          * md_format_gene uploads -> "gene"
        Confirm with the user when uncertain. Modules without an
        EntityType field (heading, page_break, text) reject this arg.
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
    try:
        structured_overlay = _resolve_dataset_settings(
            item_id=item_id,
            dataset_id=dataset_id,
            dataset_ids=dataset_ids,
            upload_id=upload_id,
            upload_ids=upload_ids,
            entity_type=entity_type,
        )
    except ValueError as e:
        return f"Error: {e}"

    # Field-type-level fallbacks for required-no-registry-default fields
    # whose JS instruction layer in the workflow repo picks a concrete
    # shape (e.g. xAxis on cv_distribution_plot defaults to
    # [{field:'sample_name', order:'none'}] in the
    # OrderableSampleMetadataColumns instruction). Layered BELOW user
    # settings so the LLM can still override.
    module = get_client().module_registry.get(item_id)
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
        mod = get_client().workspaces.modules.create_with_defaults(
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
        Note: plot modules need height >= 12 to render properly; smaller
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


# Behavioural mandates — visualisation Q&A on add+update; destructive on remove.
# Note: add_text_module / update_text_module deliberately bypass the
# visualisation mandate — the body is the user's own content, not a
# parameter that needs the platform-default vs LLM-recommendation table.
_attach_visualisation(add_module_to_tab, update_tab_module)
_attach_destructive(remove_module_from_tab)
