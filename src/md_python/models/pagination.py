"""
Pagination envelope types shared by paginated v2 resource listings.

The v2 API wraps paginated collections in ``{"data": [...], "pagination": {...}}``.
:class:`Page` is generic over the decoded element type (e.g. ``Page[Workspace]``).
"""

from typing import Generic, List, TypedDict, TypeVar

T = TypeVar("T")


class Pagination(TypedDict, total=False):
    """Pagination envelope returned alongside a paginated ``data`` list.

    All keys are optional: some endpoints return the full set, others only a
    subset (e.g. ``current_page`` + ``total_pages``).
    """

    current_page: int
    per_page: int
    total_count: int
    total_pages: int


class Page(TypedDict, Generic[T]):
    """A paginated response envelope: ``{"data": [...], "pagination": {...}}``."""

    data: List[T]
    pagination: Pagination
