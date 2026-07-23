from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass
@dataclass
class Job:
    """A runnable dataset job / analysis flow from ``GET /jobs``."""

    name: str
    id: Optional[UUID] = None
    slug: Optional[str] = None
    flow_name: Optional[str] = None
    run_type: Optional[str] = None
    is_published: bool = False
    is_custom: bool = False
    description: Optional[str] = None
    # The form schema the UI renders: {field_name: {name, group, fieldType, ...}}.
    properties: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        lines = [f"Job: {self.name}"]
        if self.id:
            lines.append(f"ID: {self.id}")
        if self.slug:
            lines.append(f"Slug: {self.slug}")
        if self.run_type:
            lines.append(f"Run Type: {self.run_type}")
        if self.flow_name:
            lines.append(f"Flow: {self.flow_name}")
        lines.append(f"Published: {self.is_published}")
        lines.append(f"Custom: {self.is_custom}")
        if self.properties:
            lines.append(f"Parameters: {len(self.properties)} fields")
        return "\n".join(lines)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Job":
        return cls(
            id=UUID(data["id"]) if data.get("id") else None,
            name=str(data.get("name", "")),
            slug=data.get("slug"),
            flow_name=data.get("flow_name"),
            run_type=data.get("run_type"),
            is_published=bool(data.get("isPublished", False)),
            is_custom=bool(data.get("is_custom", False)),
            description=data.get("description"),
            properties=dict(data.get("properties") or {}),
        )
