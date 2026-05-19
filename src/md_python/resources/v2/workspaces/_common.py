"""Shared helpers for the workspaces resource sub-package."""

from typing import Any, Dict

_PAGE_RESPONSE = Dict[str, Any]
_JSON_HEADERS = {"Content-Type": "application/json"}


def _check(response: Any, expected: int, action: str) -> None:
    if response.status_code != expected:
        raise Exception(f"Failed to {action}: {response.status_code} - {response.text}")
