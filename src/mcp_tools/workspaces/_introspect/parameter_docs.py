"""Per-parameter documentation rendering for the LLM.

Turns each ``input_settings`` entry into a richly-documented dict —
field-type profile, default-presence notes, conditional-visibility
clauses, cross-field references, and option lists.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from md_python.models import RegisteredModule

from .field_profiles import _profile_for

_NULL_DEFAULT_NOTE = (
    "default is explicitly null — the API persists null and the rendered "
    "widget shows 'Please provide ...' until the LLM/user supplies a value"
)


_MISSING_DEFAULT_NOTE = (
    "no default declared — leaving this unset means the API persists "
    "nothing for this key and the rendered widget will surface a "
    "'Please provide ...' prompt"
)


def _is_required(spec: Dict[str, Any]) -> bool:
    if spec.get("required") is True:
        return True
    rules = spec.get("rules")
    if isinstance(rules, list):
        for rule in rules:
            if isinstance(rule, dict) and rule.get("name") == "is_required":
                return True
    return False


def _resolve_refs(parameters: Optional[Dict[str, Any]]) -> List[str]:
    """Surface cross-field {ref: '<other_key>'} dependencies as plain text."""
    if not isinstance(parameters, dict):
        return []
    refs: List[str] = []
    for k, v in parameters.items():
        if isinstance(v, dict) and "ref" in v:
            refs.append(
                f"this field's value-space comes from the field {v['ref']!r} "
                f"(parameters.{k}.ref); resolve {v['ref']!r} first"
            )
    return refs


def _options_summary(
    parameters: Optional[Dict[str, Any]],
) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(parameters, dict):
        return None
    opts = parameters.get("options")
    if not isinstance(opts, list):
        return None
    out: List[Dict[str, Any]] = []
    for opt in opts:
        if isinstance(opt, dict):
            out.append({"value": opt.get("value"), "label": opt.get("name")})
    return out or None


def _condition_text(spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Turn a `when` clause into a human-readable {field, predicate, value}."""
    when = spec.get("when")
    if not isinstance(when, dict):
        return None
    if "equals" in when:
        return {
            "depends_on": when.get("property"),
            "predicate": "equals",
            "value": when["equals"],
            "human": (
                f"only set when {when.get('property')!r} == " f"{when['equals']!r}"
            ),
        }
    if "not_equals" in when:
        return {
            "depends_on": when.get("property"),
            "predicate": "not_equals",
            "value": when["not_equals"],
            "human": (
                f"only set when {when.get('property')!r} != " f"{when['not_equals']!r}"
            ),
        }
    return {
        "depends_on": when.get("property"),
        "predicate": "custom",
        "raw": when,
        "human": f"only set when condition is met (raw: {when})",
    }


def _default_note(spec: Dict[str, Any]) -> Optional[str]:
    if "default" in spec:
        if spec["default"] is None:
            return _NULL_DEFAULT_NOTE
        return None
    return _MISSING_DEFAULT_NOTE


def parameter_doc(key: str, spec: Any) -> Dict[str, Any]:
    """Fully-populated doc for one input_settings entry.

    Always returns every key documented in the schema below, even when the
    value is None — the LLM is expected to read this verbatim and present
    it to the user without skipping rows.
    """
    if not isinstance(spec, dict):
        # Some dict-shape entries can be primitives (e.g. raw default-only
        # specs); represent them as opaque-but-documented rows.
        return {
            "key": key,
            "name": key,
            "group": None,
            "field_type": None,
            "value_kind": "literal",
            "value_description": (
                "Spec is a literal value — no schema. Treat as opaque; the "
                "literal IS the platform default."
            ),
            "platform_default": spec,
            "default_present": spec is not None,
            "default_note": None,
            "is_required": False,
            "data_dependencies": [],
            "cross_field_refs": [],
            "options": None,
            "condition": None,
            "description": None,
            "fillable_by_llm": False,
            "raw_parameters": None,
        }

    field_type = spec.get("fieldType") or spec.get("type")
    profile = _profile_for(field_type)
    refs = _resolve_refs(spec.get("parameters"))

    return {
        "key": key,
        "name": spec.get("name", key),
        "group": spec.get("group"),
        "field_type": field_type,
        "value_kind": profile["value_kind"],
        "value_description": profile["value_description"],
        "platform_default": spec.get("default") if "default" in spec else None,
        "default_present": "default" in spec and spec.get("default") is not None,
        "default_note": _default_note(spec),
        "is_required": _is_required(spec),
        "data_dependencies": profile["data_dependencies"],
        "cross_field_refs": refs,
        "options": _options_summary(spec.get("parameters")),
        "condition": _condition_text(spec),
        "description": spec.get("description"),
        "fillable_by_llm": profile["fillable_by_llm"],
        "raw_parameters": spec.get("parameters"),
    }


def parameters_for(module: RegisteredModule) -> List[Dict[str, Any]]:
    """List of parameter docs in declaration order.

    Handles both wire encodings:
      * dict shape — {key: spec}; iteration order preserved (Python 3.7+).
      * array shape — list of {key, ...}; preserves array order.
    """
    schema = module.input_settings
    if not schema:
        return []
    if isinstance(schema, dict):
        return [parameter_doc(str(k), v) for k, v in schema.items()]
    out: List[Dict[str, Any]] = []
    for entry in schema:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        if key is None:
            continue
        out.append(parameter_doc(str(key), entry))
    return out


def _aggregate_data_dependencies(
    params: Sequence[Dict[str, Any]],
) -> List[str]:
    seen: List[str] = []
    for p in params:
        for dep in p.get("data_dependencies") or []:
            if dep not in seen:
                seen.append(dep)
    return seen
