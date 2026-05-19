"""Pre-flight validation for ``add_module_to_tab``.

The dataset / entity_type / arity checks live here so the MCP tool module
stays small. Functions take an already-resolved ``client`` (so the tool
layer's ``get_client()`` patch target keeps working) and return a settings
overlay ready to merge into the persisted hash.
"""

from __future__ import annotations

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


def resolve_dataset_settings(
    *,
    client: Any,
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
