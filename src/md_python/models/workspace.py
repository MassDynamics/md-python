"""
Workspace, Tab, and TabModule models for the v2 workspaces API.

These mirror the `present_workspace`, `present_tab`, and `present_tab_module`
helpers in `Api::V2::Workspaces::Api` (see `app/api/api/v2/workspaces/api.rb`).

Note on grid coordinates: the API surface uses ``height``/``width`` on
create/update, but the persisted JSON returned by the server uses the
react-grid-layout shorthand ``h``/``w`` (and ``i`` mirroring ``id``). The
``TabModule.from_json`` constructor normalises both shapes back to
``height``/``width`` to keep the client surface consistent.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is not None and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


@pydantic_dataclass
@dataclass
class Workspace:
    """A workspace — top-level container for tabs."""

    id: UUID
    name: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Workspace":
        return cls(
            id=UUID(data["id"]),
            name=data.get("name", ""),
            description=data.get("description"),
            created_at=_parse_iso_datetime(data.get("created_at")),
            updated_at=_parse_iso_datetime(data.get("updated_at")),
        )


@pydantic_dataclass
@dataclass
class Tab:
    """A tab inside a workspace — holds a layout of modules."""

    id: UUID
    workspace_id: UUID
    name: str
    settings: Dict[str, Any]
    tab_index: int
    locked: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Tab":
        return cls(
            id=UUID(data["id"]),
            workspace_id=UUID(data["workspace_id"]),
            name=data.get("name", ""),
            settings=data.get("settings") or {},
            tab_index=int(data.get("tab_index", 0)),
            locked=bool(data.get("locked", False)),
            created_at=_parse_iso_datetime(data.get("created_at")),
            updated_at=_parse_iso_datetime(data.get("updated_at")),
        )


@pydantic_dataclass
@dataclass
class TabModule:
    """A module placed on a tab's grid.

    The API surface uses ``height``/``width``; the underlying persistence uses
    react-grid-layout shorthand (``h``/``w``). ``from_json`` accepts either.
    """

    id: UUID
    item_id: str
    height: int
    width: int
    x: int
    y: int
    settings: Dict[str, Any]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "TabModule":
        height = data.get("height", data.get("h"))
        width = data.get("width", data.get("w"))
        if height is None or width is None:
            raise ValueError(
                "TabModule JSON must contain height/width (or h/w); "
                f"got keys: {sorted(data.keys())}"
            )

        item_id = data.get("item_id") or data.get("itemId")
        if not item_id:
            raise ValueError("TabModule JSON missing item_id/itemId")

        return cls(
            id=UUID(data["id"]),
            item_id=str(item_id),
            height=int(height),
            width=int(width),
            x=int(data["x"]),
            y=int(data["y"]),
            settings=data.get("settings") or {},
        )
