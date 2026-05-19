"""LLM behavioural mandate for the visualisation tools.

Placing a module on a tab is the visual analogue of running a pipeline: the
LLM must walk the user through every parameter before submitting. Same
shape as ``mcp_tools.pipelines._mandates`` but tuned for the visualisation
loop where most parameters are non-statistical (sample-metadata pickers,
list selectors, plot configuration).

Two extra rules apply on top of the pipeline mandates:

  3. DATA-DEPENDENCY DISCLOSURE — for every parameter whose
     ``fillable_by_llm`` is False (Datasets, EntityType, ProteinList,
     DatasetSampleMetadata, …), the LLM MUST tell the user *what data it
     needs* before it can suggest a value, list every dependency from the
     parameter's ``data_dependencies`` block, and call the relevant fetch
     tool (e.g. ``get_upload_sample_metadata``, ``query_entities``,
     ``find_initial_dataset``) before proposing a value.

  4. CONDITIONAL-VISIBILITY DISCLOSURE — if a parameter declares a
     ``condition`` block, the LLM must tell the user that the parameter
     only matters when the condition holds, and either omit the parameter
     or set it to a value consistent with the condition.

The fragment is attached to ``add_module_to_tab`` and ``update_tab_module``
docstrings at import time. The canonical set of tools that MUST carry the
visualisation mandate is enumerated in ``VISUALISATION_MANDATE_TOOL_NAMES``
below — single source of truth, mirrored by the test in
tests/mcp_tools/test_mandate_wiring.py. Add a new visualisation-mandate tool?
Update the set here AND call ``_attach_visualisation`` from its module, or
the regression test fails.

Note on scope: only ``add_module_to_tab`` and ``update_tab_module`` carry
this mandate. The text-module and plotly-json-module tools deliberately do
NOT — their only user-supplied value is the body / figure itself, so there
is no platform-default vs LLM-recommendation table to walk through.
"""

# Canonical set of visualisation-mandate tool names. The mandate applies to
# tools that PLACE or RECONFIGURE a parameterised module — i.e. anything that
# would benefit from the two-defaults Q&A. Pure content modules (text,
# plotly-json) are excluded; see the docstring above.
VISUALISATION_MANDATE_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "add_module_to_tab",
        "update_tab_module",
    }
)

VISUALISATION_MANDATE_FRAGMENT = """

══════════════════════════════════════════════════════════════════════════════
LLM BEHAVIOURAL MANDATES — VISUALISATION (binding)
══════════════════════════════════════════════════════════════════════════════

(1) MANDATORY PARAMETER Q&A — ASK BEFORE PLACING THE MODULE.
Before calling this tool, the LLM MUST:
  * Call describe_module_type(item_id) and read every parameter — even those
    whose default is null, even those marked optional.
  * Present every parameter in a single table to the user. Never elide rows.
  * Wait for the user's explicit confirmation. Silence is NOT confirmation.
  * Use create_with_defaults semantics: every key the registry declares a
    default for is sent on the wire even if the user does not change it.

(2) TWO-DEFAULTS MANDATE — TWO COLUMNS, NOT ONE.
The parameter table MUST contain exactly two defaults columns:

  PLATFORM DEFAULT
    The canonical Mass Dynamics default returned by describe_module_type
    (parameter.platform_default). When ``default_note`` is set, quote it —
    e.g. "default is explicitly null — the rendered widget shows
    'Please provide ...' until set" or "no default declared".

  LLM RECOMMENDATION
    What the LLM thinks the user should use given the experiment context
    (entity type, sample design, available metadata columns). Justify in
    one sentence. When the recommendation DIVERGES from the platform
    default, mark the row with "(diverges)" and explain why in one extra
    sentence.

The LLM MUST NOT silently auto-pick. Pre-filled recommended values are fine
ONLY when explicitly labelled "LLM recommendation, please confirm or change."

(3) DATA-DEPENDENCY DISCLOSURE — DECLARE WHAT YOU NEED FIRST.
For every parameter whose ``fillable_by_llm`` is False (Datasets,
EntityType, ProteinList(s), ProteinSelection, DatasetSampleMetadata,
DatasetSampleMetadataValues, OrderableSampleMetadataColumns,
SampleMetadataValuesFilter, ConditionComparison), the LLM MUST:
  * Read the parameter's ``data_dependencies`` block verbatim and tell the
    user what data is needed before a value can be picked.
  * Fetch that data via the appropriate tool BEFORE proposing values:
      - dataset id of correct type            → find_initial_dataset /
                                                 list_datasets / query_datasets
      - sample-metadata column or values      → get_upload_sample_metadata
      - protein-group ids / entity references → query_entities
      - condition pair on a PAIRWISE dataset  → list_datasets +
                                                 inspect Dataset.job_run_params
  * If a dependency cannot be resolved (e.g. an entity-list id that lives
    only in the app), say so explicitly and ask the user to provide it.

(4) CONDITIONAL-VISIBILITY DISCLOSURE.
For every parameter whose ``condition`` block is non-null, tell the user the
parameter only applies when the condition holds (e.g. "correlationMethod
only matters when dataType=='correlation'") and either omit the parameter
or set it to a value consistent with the surrounding choices.

(5) NO PARAMETER LEFT UNDOCUMENTED.
If any parameter row is missing from your table, you have violated this
mandate. Even null defaults, even optional parameters, even fields whose
fieldType is unmapped (value_kind="unmapped:..." or "unknown") MUST appear
in the table with a row labelled "TODO — fieldType unmapped, ask the user".
══════════════════════════════════════════════════════════════════════════════
"""


def _attach_visualisation(*funcs: object) -> None:
    """Append VISUALISATION_MANDATE_FRAGMENT to each tool's __doc__.

    Idempotent — re-importing the module will not double-attach.
    """
    for fn in funcs:
        doc = getattr(fn, "__doc__", None) or ""
        if "LLM BEHAVIOURAL MANDATES — VISUALISATION" in doc:
            continue
        try:
            fn.__doc__ = doc.rstrip() + "\n" + VISUALISATION_MANDATE_FRAGMENT  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            pass
