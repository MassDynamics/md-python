"""Pre-flight validation for ``add_module_to_tab``.

The dataset / entity_type / arity checks live here so the MCP tool module
stays small. Functions take an already-resolved ``client`` (so the tool
layer's ``get_client()`` patch target keeps working) and return a
``Resolution`` — the settings overlay to merge into the persisted hash,
plus non-fatal ``warnings`` the tool surfaces in its success payload.

Fail-fast vs warn:
  * FAIL when the module cannot be created correctly from what the LLM
    passed (wrong dataset type, wrong arity, unresolvable required
    entity_type, unknown comparison).
  * WARN when the argument is simply surplus to this module's spec and
    dropping it yields the module the LLM asked for (e.g. entity_type on
    a module with no EntityType field). Never silently swallow — the
    warning goes in the success payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from md_python.models import TabModule

from . import _introspect


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


@dataclass
class Resolution:
    """Outcome of pre-flight validation.

    ``overlay`` is merged into the persisted settings hash (it wins over
    user-supplied settings). ``warnings`` are non-fatal notes the tool
    MUST surface to the LLM in the success payload — a dropped argument,
    or an entity_type we inferred on the LLM's behalf.
    """

    overlay: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


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


def _validate_entity_type_arg(
    item_id: str,
    entity_type: Optional[str],
    eti: Optional[Dict[str, Any]],
    res: Resolution,
) -> None:
    """Validate an entity_type the LLM DID supply, against the module spec.

    Three outcomes:
      * module has no EntityType field — there is nothing to set, so the
        arg is DROPPED with a warning (surfaced in the success payload).
        Hard-failing here used to reject perfectly valid module
        placements: the LLM cannot know which class a module is in
        without a prior describe_module_type call.
      * value not in the module's own ``valid_values`` — hard fail (the
        persisted value would be nonsense). The valid values quoted are
        the MODULE's, never a global list.
      * value valid — merged into the overlay under the field's key.

    A missing entity_type is NOT handled here; see
    ``_resolve_required_entity_type`` (it needs the dataset in hand).
    """
    if eti is None:
        if entity_type is not None:
            res.warnings.append(
                f"entity_type={entity_type!r} was DROPPED: module "
                f"{item_id!r} has no EntityType-typed parameter in its "
                "registry spec, so there is nothing to set it on. The "
                "module was created without it. Check "
                "entity_type_input in list_module_types / "
                "describe_module_type before passing entity_type."
            )
        return
    if entity_type is None:
        return
    if entity_type not in eti["valid_values"]:
        raise ValueError(
            f"entity_type must be one of {eti['valid_values']} for module "
            f"{item_id!r}, got {entity_type!r}. These are the values THIS "
            "module accepts (published as entity_type_input.valid_values "
            "by list_module_types / describe_module_type) — the accepted "
            "set differs between modules."
        )
    res.overlay[eti["settings_key"]] = entity_type


def _infer_entity_type(
    *,
    client: Any,
    datasets: List[Any],
    upload_ids: List[str],
) -> Optional[tuple]:
    """Best-effort entity_type inference, or None when it is not safe.

    Two signals, strongest first:

    1. ``dataset.job_run_params['entity_type']`` — the SAME signal the web
       UI reads (EntityTypeSelectField.vue narrows the select to a single
       option when the dataset carries it). Every pipeline-produced
       dataset (NI / pairwise / ANOVA / GSEA / ORA / WGCNA) persists it.
    2. The upload's ``source`` — but ONLY for the sources that map 1:1
       onto an entity_type (md_format_gene, md_format_metabolite). The
       proteomics sources can be protein OR peptide, which is exactly the
       choice we must not make for the user.

    Returns ``(entity_type, provenance)`` or None. Disagreement across
    multiple datasets/uploads is treated as "not inferable" rather than
    picking one.
    """
    from_datasets = {
        et
        for et in (
            _introspect.entity_type_from_dataset(getattr(ds, "job_run_params", None))
            for ds in datasets
        )
        if et is not None
    }
    if len(from_datasets) == 1:
        return (
            from_datasets.pop(),
            "the dataset's job_run_params.entity_type (the same signal the "
            "web UI uses to pick the entity type for this dataset)",
        )
    if from_datasets:
        return None

    sources = {
        src for src in (_upload_source(client, uid) for uid in upload_ids) if src
    }
    from_sources = {
        et
        for et in (_introspect.entity_type_from_upload_source(s) for s in sources)
        if et is not None
    }
    if len(from_sources) == 1:
        return (
            from_sources.pop(),
            f"the upload's source ({', '.join(sorted(sources))}), which maps "
            "1:1 onto an entity type",
        )
    return None


def _upload_source(client: Any, upload_id: str) -> Optional[str]:
    """The upload's ``source``, or None when it cannot be fetched.

    Inference is a convenience — a failed lookup must degrade to the
    explicit "pass entity_type" error, never to an exception.
    """
    try:
        upload = client.uploads.get_by_id(upload_id)
    except Exception:
        return None
    source = getattr(upload, "source", None)
    return source if isinstance(source, str) and source else None


def _resolve_required_entity_type(
    *,
    client: Any,
    item_id: str,
    eti: Optional[Dict[str, Any]],
    entity_type: Optional[str],
    datasets: List[Any],
    upload_ids: List[str],
    res: Resolution,
) -> None:
    """Fill a REQUIRED entity_type the LLM did not supply — or fail.

    Runs after the dataset is resolved, so the dataset (and, failing
    that, its upload) can be interrogated. When neither yields an
    unambiguous answer we keep hard-failing: guessing protein for a
    metabolite dataset renders the wrong table, which is worse than an
    error the LLM can act on.
    """
    if eti is None or entity_type is not None or not eti["required"]:
        return

    inferred = _infer_entity_type(
        client=client, datasets=datasets, upload_ids=upload_ids
    )
    if inferred is not None and inferred[0] in eti["valid_values"]:
        value, provenance = inferred
        res.overlay[eti["settings_key"]] = value
        res.warnings.append(
            f"entity_type was not supplied; INFERRED {value!r} from "
            f"{provenance}. Tell the user which entity type the module was "
            "created with, and pass entity_type explicitly if that is wrong."
        )
        return

    raise ValueError(
        f"module {item_id!r} requires entity_type — one of "
        f"{eti['valid_values']} (this module's own accepted set; it "
        "differs between modules). It could NOT be inferred: the chosen "
        "dataset carries no job_run_params.entity_type and the upload's "
        "source does not map to a single entity type. Supply it: "
        "protein/peptide for md_format / DIA-NN / MaxQuant / Spectronaut "
        "uploads (which one depends on the table you analysed), gene for "
        "md_format_gene uploads, metabolite for md_format_metabolite "
        "uploads. Confirm with the user when uncertain.\n\n"
        "To know a module's entity_type contract BEFORE calling this tool, "
        "read entity_type_input — list_module_types publishes it for every "
        "module, and describe_module_type(item_id) documents it in full. "
        "entity_type_input=null means the module takes no entity_type."
    )


def resolve_dataset_settings(
    *,
    client: Any,
    item_id: str,
    dataset_id: Optional[str],
    dataset_ids: Optional[List[str]],
    upload_id: Optional[str],
    upload_ids: Optional[List[str]],
    entity_type: Optional[str],
    comparison: Optional[List[str]] = None,
) -> Resolution:
    """Validate dataset + entity_type args against the module's spec and
    return the settings overlay (+ warnings) for ``settings``.

    Raises ``ValueError`` with a clear message on every shape mismatch:
      * arity mismatch (single vs multiple) vs the registry's
        ``parameters.multiple`` flag,
      * dataset.type mismatch vs ``parameters.type`` (the most common
        symptom of the LLM confusing upload_id with dataset_id),
      * missing companion upload_id(s),
      * companion list-length mismatch,
      * dataset args passed to a module that does NOT have a Datasets
        field (e.g. ``heading``, ``page_break``),
      * entity_type required, not supplied, and not inferable from the
        dataset / upload,
      * entity_type not in the module's own ``valid_values``.

    Non-fatal (warning + carry on):
      * entity_type passed to a module that has no EntityType field — the
        arg is dropped,
      * entity_type inferred from the dataset / upload.
    """
    module = client.module_registry.get(item_id)
    if module is None:
        raise ValueError(
            f"item_id {item_id!r} is not in the module registry "
            "(or is not available to the current user)"
        )

    res = Resolution()
    eti = _introspect.entity_type_input_for(module)
    _validate_entity_type_arg(item_id, entity_type, eti, res)

    datasets = _resolve_dataset_overlay(
        client=client,
        item_id=item_id,
        module=module,
        dataset_id=dataset_id,
        dataset_ids=dataset_ids,
        upload_id=upload_id,
        upload_ids=upload_ids,
        comparison=comparison,
        res=res,
    )

    # Deferred until the dataset is in hand: a required-but-absent
    # entity_type may be inferable from it (or from its upload).
    _resolve_required_entity_type(
        client=client,
        item_id=item_id,
        eti=eti,
        entity_type=entity_type,
        datasets=datasets,
        upload_ids=[upload_id] if upload_id else list(upload_ids or []),
        res=res,
    )
    return res


def _resolve_dataset_overlay(
    *,
    client: Any,
    item_id: str,
    module: Any,
    dataset_id: Optional[str],
    dataset_ids: Optional[List[str]],
    upload_id: Optional[str],
    upload_ids: Optional[List[str]],
    comparison: Optional[List[str]],
    res: Resolution,
) -> List[Any]:
    """Merge the dataset envelope (+ pairwise comparison) into ``res``.

    Returns the ``Dataset`` objects that were bound — the caller uses them
    to infer a missing entity_type. Empty list when the module has no
    Datasets field or none was passed.
    """
    has_id_arg = dataset_id is not None
    has_ids_arg = dataset_ids is not None
    has_any = has_id_arg or has_ids_arg
    overlay = res.overlay

    di = _introspect.dataset_input_for(module)

    # Pairwise modules (volcano) carry a ConditionComparison field that
    # selects which case-vs-control pair to plot and which side is left /
    # right of the log2 ratio. It is required-no-default, so we resolve it
    # from the chosen PAIRWISE dataset's job_run_params below. Detect it
    # up-front so we can reject a `comparison` arg on a module that has no
    # such field.
    cci = _introspect.condition_comparison_input_for(module)
    if comparison is not None and cci is None:
        raise ValueError(
            f"module {item_id!r} does not accept a comparison (no "
            "ConditionComparison-typed parameter in its registry spec); "
            "drop comparison"
        )

    # Module has no Datasets field (heading, page_break, text, …).
    if di is None:
        if has_any:
            raise ValueError(
                f"module {item_id!r} does not accept a dataset (no "
                "Datasets-typed parameter in its registry spec); drop "
                "dataset_id / dataset_ids"
            )
        return []

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
        return []

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

        # Resolve the pairwise comparison from the same dataset so the
        # volcano renders with the correct conditionPair + left/right
        # groups instead of an empty default.
        if cci is not None:
            pairs = _introspect._condition_comparison_pairs(ds.job_run_params)
            overlay[cci["settings_key"]] = _introspect.build_condition_comparison(
                pairs, comparison
            )
        return [ds]

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
    bound: List[Any] = []
    for did, uid in zip(dataset_ids, upload_ids):
        ds = client.datasets.get_by_id(did)
        if ds is None:
            raise ValueError(
                f"dataset_id {did!r} not found (or no permission). "
                "Did you pass an upload_id by mistake?"
            )
        _check_dataset_type(str(ds.id), ds.type, di["dataset_type"])
        entries.append({"id": str(ds.id), "name": ds.name, "upload_id": uid})
        bound.append(ds)
    envelope = _introspect.build_dataset_envelope_multi(
        entries, dataset_type=di["dataset_type"]
    )
    overlay[di["settings_key"]] = envelope
    return bound
