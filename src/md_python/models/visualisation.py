"""
Visualisation result types for the module ``visualisation`` endpoint.

The server renders a module into a Plotly-style figure. Rendering is async:
a request returns either the finished :class:`PlotlyVisualisation` payload or,
while still rendering, a :class:`VisualisationPending` telling the caller when
to retry.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypedDict


class PlotlyVisualisation(TypedDict):
    """A rendered Plotly figure: a list of traces plus a layout dict."""

    data: List[Dict[str, Any]]
    layout: Dict[str, Any]


@dataclass
class VisualisationPending:
    """Returned when a visualisation is still rendering after the wait timeout.

    Retry later — after ``retry_after`` seconds when the server supplied that
    hint, otherwise at the caller's discretion.
    """

    retry_after: Optional[int] = None
