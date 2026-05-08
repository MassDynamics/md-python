"""
RegisteredModule model for the v2 module_registry API.

Mirrors the `to_manifest_hash` shape of `ModuleRegistry::RegisteredModule`
(see `app/models/module_registry/registered_module.rb`), minus the
``availability`` key which the API strips out.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic.dataclasses import dataclass as pydantic_dataclass

# input_settings on the wire is either a list of {key, type, ...} dicts or a
# {key: spec, ...} mapping — the Ruby side accepts both shapes (see
# `Workspaces::TabModule#setting_keys_for`).
InputSettings = Union[List[Dict[str, Any]], Dict[str, Any]]


def _spec_is_required(spec: Any) -> bool:
    """Detect 'required' across both wire encodings.

    Array-shape entries carry a top-level ``required: true`` boolean; live
    dict-shape entries carry a ``rules`` list with a ``{name: 'is_required'}``
    rule (and may *also* carry the boolean — accept either).
    """
    if not isinstance(spec, dict):
        return False
    if spec.get("required") is True:
        return True
    rules = spec.get("rules")
    if isinstance(rules, list):
        for rule in rules:
            if isinstance(rule, dict) and rule.get("name") == "is_required":
                return True
    return False


def _condition_met(spec: Any, settings: Dict[str, Any]) -> bool:
    """Evaluate the ``when`` clause on a field spec against the merged
    settings. A field with no ``when`` clause is always 'visible' (True).

    Mirrors what the React renderer does to decide whether to show the
    field — modules like ``customTitle`` are required-no-default but
    only surface when their parent field (``titleDisplay``) takes a
    specific value (``'custom'``). Without honouring this, a sibling
    required-no-default field would always appear missing even when the
    UI hides it.
    """
    if not isinstance(spec, dict):
        return True
    when = spec.get("when")
    if not isinstance(when, dict):
        return True
    prop = when.get("property")
    if prop is None:
        return True
    actual = settings.get(prop)
    if "equals" in when:
        return bool(actual == when["equals"])
    if "not_equals" in when:
        return bool(actual != when["not_equals"])
    # Unknown predicate shape — be conservative: assume visible. A bad
    # call to the API here is preferable to silently dropping a required
    # field on a never-seen condition shape.
    return True


@pydantic_dataclass
@dataclass
class RegisteredModule:
    """A dashboard module type from the registry manifest."""

    id: str
    name: str
    group: str
    icon: str
    short_name: Optional[str] = None
    description: Optional[str] = None
    short_description: Optional[str] = None
    keywords: List[str] = field(default_factory=list)
    instruction_name: Optional[str] = None
    input_settings: Optional[InputSettings] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "RegisteredModule":
        return cls(
            id=str(data["id"]),
            name=str(data.get("name", "")),
            group=str(data.get("group", "")),
            icon=str(data.get("icon", "")),
            short_name=data.get("shortName"),
            description=data.get("description"),
            short_description=data.get("shortDescription"),
            keywords=list(data.get("keywords") or []),
            instruction_name=data.get("instructionName"),
            input_settings=data.get("input_settings"),
        )

    def setting_keys(self) -> List[str]:
        """Allowed top-level keys in a TabModule.settings hash for this module.

        The server validates ``settings`` against this list (extra keys → 400).
        Returns an empty list if the module declares no input_settings (in
        which case the server skips the keys check).
        """
        schema = self.input_settings
        if not schema:
            return []
        if isinstance(schema, dict):
            return [str(k) for k in schema.keys()]
        return [str(s.get("key")) for s in schema if s.get("key") is not None]

    def required_setting_keys(self) -> List[str]:
        """Keys flagged required in input_settings.

        Two encodings appear on the wire and both are handled:

        * **Array shape** (cached manifest at
          ``app/assets/builds/module-registry-manifest.json``) —
          ``{"key": "text", "required": true, ...}``.
        * **Dict shape** (live ``GET /module_registry/modules/:id``) —
          ``{"text": {"rules": [{"name": "is_required"}], ...}}``.
        """
        schema = self.input_settings
        if not schema:
            return []
        if isinstance(schema, dict):
            return [str(k) for k, v in schema.items() if _spec_is_required(v)]
        return [
            str(s["key"])
            for s in schema
            if s.get("key") is not None and _spec_is_required(s)
        ]

    def defaults(self) -> Dict[str, Any]:
        """Return ``{key: default}`` for every input_setting that declares one.

        Skips entries whose ``default`` is ``None`` (the registry's "no default"
        sentinel). For required-without-default keys (e.g. ``heading.text``)
        the caller still has to provide a value explicitly.

        Use this to build a complete ``settings`` payload for module create —
        the API doesn't merge registry defaults server-side, so a partial
        payload persists as-is and the frontend ends up rendering broken
        widgets ("Please provide Size", etc.).
        """
        schema = self.input_settings
        if not schema:
            return {}
        if isinstance(schema, dict):
            return {
                str(k): v["default"]
                for k, v in schema.items()
                if isinstance(v, dict) and v.get("default") is not None
            }
        return {
            str(s["key"]): s["default"]
            for s in schema
            if s.get("key") is not None and s.get("default") is not None
        }

    def missing_required_keys(self, settings: Dict[str, Any]) -> List[str]:
        """Required keys that are *not* satisfied by ``settings`` or by the
        registry's own defaults — and whose ``when`` clause is currently met.

        Returns the offending keys so the caller can fail fast before
        round-tripping to the API. A required key with a registry default
        is considered satisfied (the client helper will fill it in). A
        required key whose ``when`` clause evaluates False (the field is
        hidden in the UI given the current settings + defaults) is also
        considered satisfied — the renderer never asks for it.
        """
        required = set(self.required_setting_keys())
        if not required:
            return []
        # Merge defaults under user settings to compute the "effective"
        # state the renderer will see; conditions are evaluated against
        # this merged view, NOT just user-supplied values.
        effective: Dict[str, Any] = {}
        effective.update(self.defaults())
        effective.update(settings)

        satisfied = set(effective.keys())
        # Drop keys whose `when` clause says they are hidden under the
        # effective settings — the UI does not surface them and the
        # renderer does not need them.
        schema = self.input_settings
        spec_for: Dict[str, Any] = {}
        if isinstance(schema, dict):
            spec_for = {str(k): v for k, v in schema.items()}
        elif isinstance(schema, list):
            spec_for = {
                str(s["key"]): s
                for s in schema
                if isinstance(s, dict) and s.get("key") is not None
            }

        relevant = {
            key for key in required if _condition_met(spec_for.get(key), effective)
        }
        return sorted(relevant - satisfied)

    def validate_settings_keys(self, settings: Dict[str, Any]) -> Sequence[str]:
        """Return any keys in ``settings`` not declared in the manifest.

        Mirrors the ``settings_keys_must_match_registered_module`` check in
        ``Workspaces::TabModule``. Returns the offending keys (sorted) so
        callers can fail fast before round-tripping to the API.
        """
        allowed = set(self.setting_keys())
        if not allowed:
            return []
        unknown = sorted(str(k) for k in settings.keys() if str(k) not in allowed)
        return unknown
