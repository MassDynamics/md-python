"""Which module types the visualisation API can actually RENDER.

A module being *placeable* on a tab (it is in the module registry) does
NOT mean it is *renderable* through
``GET /workspaces/:ws/tabs/:tab/modules/:id/visualisation``. The Rails app
dispatches that endpoint through a frozen REGISTRY — anything not in it
raises ``UnsupportedModuleError`` ("Visualisation not supported for module
type '<id>'"). Every other module draws in the web UI only (the browser
builds the figure client-side from the module's settings).

Telemetry (2026-07) showed this as a top failure mode: the LLM
successfully places e.g. ``gsea_dot_plot`` / ``pairwise_heatmap`` /
``qc_summary_table``, then burns a failing round-trip on
render_module_visualisation because nothing up front says the module is
not renderable.

SOURCE OF TRUTH — MIRRORED, NOT DERIVED
  ``workflow/app/services/visualisations/service_client.rb`` ::

      REGISTRY = {
        'pairwise_volcano_plot'        => Visualisations::PairwiseVolcanoPlot,
        'instrument_qc_bar_chart'      => Visualisations::InstrumentQcBarChart,
        ...
      }.freeze

The Rails app exposes no endpoint that publishes this list, so the MCP
mirrors it as a constant. It MUST be kept in sync by hand whenever a
visualisation is added to (or removed from) that REGISTRY — the workflow
repo is not editable from here. Last synced: 2026-07-14 (12 entries).
"""

from __future__ import annotations

from typing import List

# Mirrors Visualisations::ServiceClient::REGISTRY key order verbatim.
RENDERABLE_MODULE_IDS: List[str] = [
    "pairwise_volcano_plot",
    "instrument_qc_bar_chart",
    "box_plot",
    "cv_distribution_plot",
    "cv_distribution_violin_plot",
    "missing_values_by_sample_plot",
    "missing_values_by_feature_plot",
    "intensity_distribution_plot",
    "missing_values_heatmap",
    "entity_detection_coverage_plot",
    "ptm_intensity_scatter",
    "entity_abundance_plot",
]

_RENDERABLE_SET = frozenset(RENDERABLE_MODULE_IDS)

# Said to the LLM whenever a module is placeable but not renderable. The
# module is NOT broken — it simply has no server-side renderer.
NOT_RENDERABLE_NOTE = (
    "this module type has no server-side renderer, so "
    "render_module_visualisation will fail for it. The module IS valid and "
    "draws normally when the user opens the workspace in the browser (the "
    "figure is built client-side). Do not call render_module_visualisation "
    "on it; tell the user to open the workspace to see it."
)


def is_renderable(item_id: str) -> bool:
    """True when ``item_id`` has a server-side visualisation renderer.

    False means "placeable but UI-only" — see ``NOT_RENDERABLE_NOTE``.
    """
    return item_id in _RENDERABLE_SET
