import json

from . import mcp
from ._client import get_client


@mcp.tool()
def health_check() -> str:
    """Check the Mass Dynamics API health status."""
    result = get_client().health.check()
    return json.dumps(result, indent=2)
