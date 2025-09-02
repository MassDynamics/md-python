"""
Health check resource for the MD Python client
"""

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from ..client import MDClient


class Health:
    """Health check resource"""

    def __init__(self, client: "MDClient"):
        self._client = client

    def check(self) -> Dict[str, Any]:
        """Check the health status of the API"""
        try:
            response = self._client._make_request("GET", "/health")
            response.raise_for_status()
            result: Dict[str, Any] = response.json()
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}
