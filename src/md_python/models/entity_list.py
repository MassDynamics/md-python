"""
EntityList and EntityListItem models for the v2 workspaces entity_lists API.

Mirror the ``ToHash::EntityList.to_hash_with_owner`` helper plus the
appended ``items`` field set by ``present_entity_list`` in
``app/api/api/v2/workspaces/api.rb``.

An entity list groups together a fixed selection of proteins, peptides,
or genes drawn from one or more datasets — used by visualisation modules
that take a list-id (``proteinListId`` / ``entityListId``) instead of a
full dataset.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is not None and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


class EntityType(StrEnum):
    protein = "protein"
    peptide = "peptide"
    gene = "gene"
    metabolite = "metabolite"
    ptm = "ptm"


@pydantic_dataclass
@dataclass
class EntityListItem:
    """A single membership row in an entity list.

    ``dataset_id`` and ``group_id`` must both be set or both be null —
    the server-side validator on ``EntityListItem`` enforces this. The
    pair identifies a specific row in a specific dataset; ``entity_id``
    is the human-readable identifier (e.g. protein group accession).
    """

    entity_id: str
    group_id: Optional[int] = None
    dataset_id: Optional[str] = None
    id: Optional[UUID] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "EntityListItem":
        # Server returns ``dataset_external_id`` on the persisted record
        # but accepts ``dataset_id`` on create — normalise both back to
        # ``dataset_id`` on the Python side so callers see one name.
        raw_id = data.get("id")
        return cls(
            entity_id=str(data["entity_id"]),
            group_id=data.get("group_id"),
            dataset_id=data.get("dataset_id") or data.get("dataset_external_id"),
            id=UUID(str(raw_id)) if raw_id is not None else None,
        )

    def to_create_payload(self) -> Dict[str, Any]:
        """Render as the JSON shape the Create endpoint accepts."""
        payload: Dict[str, Any] = {"entity_id": self.entity_id}
        if self.group_id is not None:
            payload["group_id"] = self.group_id
        if self.dataset_id is not None:
            payload["dataset_id"] = self.dataset_id
        return payload


@pydantic_dataclass
@dataclass
class EntityList:
    """A named list of proteins / peptides / genes drawn from datasets."""

    id: UUID
    name: str
    type: EntityType
    experiment_id: Optional[UUID] = None
    items_count: int = 0
    owner: bool = False
    items: List[EntityListItem] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "EntityList":
        experiment_id_raw = data.get("experiment_id")
        return cls(
            id=UUID(str(data["id"])),
            name=str(data["name"]),
            type=EntityType(data["type"]),
            experiment_id=(
                UUID(str(experiment_id_raw)) if experiment_id_raw is not None else None
            ),
            items_count=int(data.get("items_count") or 0),
            owner=bool(data.get("owner", False)),
            items=[EntityListItem.from_json(item) for item in data.get("items") or []],
            created_at=_parse_iso_datetime(data.get("created_at")),
            updated_at=_parse_iso_datetime(data.get("updated_at")),
        )
