"""
ReferenceDataFile model for the v2 reference data API.

Mirrors the ``{ id, filename }`` entries returned by ``GET /reference_data``
(see ``app/api/api/v2/reference_data/list.rb``); it also identifies the file
in the ``POST /reference_data`` create response.

The ``id`` is not a bare UUID: it is ``"<org_uuid>/<file_uuid>"``, where the
``<org_uuid>`` segment scopes the file to the owning organisation. The
``organisation_uuid`` and ``file_uuid`` properties expose the two halves.
"""

from dataclasses import dataclass
from typing import Any, Dict
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass


@pydantic_dataclass
@dataclass
class ReferenceDataFile:
    """A single organisation-scoped reference data file."""

    id: str
    filename: str

    @property
    def organisation_uuid(self) -> UUID:
        """The ``<org_uuid>`` half of ``id``."""
        return UUID(self.id.split("/", 1)[0])

    @property
    def file_uuid(self) -> UUID:
        """The ``<file_uuid>`` half of ``id``."""
        return UUID(self.id.split("/", 1)[1])

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "ReferenceDataFile":
        """Build from a ``list`` entry: ``{ id, filename }``."""
        return cls(
            id=str(data["id"]),
            filename=str(data.get("filename", "")),
        )
