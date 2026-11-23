"""
Project model for the v2 projects API.

Mirrors the entries returned by ``GET /projects`` (see
``app/api/api/v2/projects/list.rb`` and ``ProjectPresenter#present_summary``).
``tags`` holds the tag names of the project's data collection; they can be
passed to ``Upload(tags=...)``.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass
@dataclass
class Project:
    """A project the current user can access."""

    id: UUID
    name: str
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"Project: {self.name}", f"ID: {self.id}"]
        if self.description:
            lines.append(f"Description: {self.description}")
        if self.tags:
            lines.append(f"Tags: {', '.join(self.tags)}")
        return "\n".join(lines)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Project":
        return cls(
            id=UUID(str(data["id"])),
            name=data.get("name", ""),
            description=data.get("description"),
            tags=list(data.get("tags") or []),
        )
