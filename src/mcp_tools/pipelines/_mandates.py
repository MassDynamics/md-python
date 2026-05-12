"""Shared LLM-behaviour mandates for analysis tool docstrings.

Two rules apply to every analysis tool (NI, pairwise, ANOVA, dose-response,
including the bulk variants where it makes sense):

1. Mandatory parameter Q&A. Before submitting, the LLM MUST present every
   parameter (required + tuneable) in a table to the user, explain what each
   does in plain language, and wait for explicit confirmation. Bulk variants
   require ONE confirmation for the whole batch.

2. Two-defaults mandate. When showing parameters, the LLM MUST show TWO
   defaults columns:
     * Platform default — the canonical Mass Dynamics default (the value sent
       if the user does nothing). Cite source-of-truth.
     * LLM-recommended default — what the LLM suggests given the experiment
       context. Justify in one sentence and explicitly mark when it diverges
       from the platform default.
   The LLM MUST NEVER silently auto-pick. It can pre-fill a recommended value
   but MUST call it out as "LLM recommendation, please confirm or change."

The strings below are imported into tool docstrings via runtime concatenation
(``run_xxx.__doc__ += MANDATES_FRAGMENT``) so a single source of truth keeps
every analysis-tool docstring aligned.
"""

# Per-tool mandates fragment — append to every analysis tool docstring.
MANDATES_FRAGMENT = """

══════════════════════════════════════════════════════════════════════════════
LLM BEHAVIOURAL MANDATES (binding — every analysis tool)
══════════════════════════════════════════════════════════════════════════════

(1) MANDATORY PARAMETER Q&A — ASK BEFORE SUBMITTING.
Before calling this tool, the LLM MUST:
  * Present every parameter (required AND tuneable) in a single table.
  * Explain in plain language what each parameter controls.
  * Wait for the user's explicit confirmation. Never auto-pick, even when a
    default exists. "OK", "yes", or "use the recommended values" all count
    as explicit confirmation; silence does not.

(2) TWO-DEFAULTS MANDATE — TWO COLUMNS, NOT ONE.
The parameter table MUST contain exactly two defaults columns:

  PLATFORM DEFAULT
    The canonical Mass Dynamics default — the value sent if the user does
    nothing. Cite the source-of-truth file:line for any non-obvious default
    (e.g. "MD-converter NormalisationAndImputationParamsProperties:
    filter_threshold_proportion=0.5").

  LLM RECOMMENDATION
    What the LLM thinks the user should use given the experiment context
    (sample size, replicates, missingness pattern, batch structure, gene
    vs protein, etc.). Justify in one sentence. When the recommendation
    DIVERGES from the platform default, mark the row with "(diverges)"
    and explain why in one extra sentence.

The LLM MUST NOT silently auto-pick. A pre-filled recommended value is fine
ONLY when explicitly labelled "LLM recommendation, please confirm or change."

Bulk-tool variant: confirmation is collected ONCE for the whole batch. The
table is shown for the parameter set that will be applied to every job in
the batch; the user confirms the whole table before submission.
══════════════════════════════════════════════════════════════════════════════
"""


def _attach(*funcs: object) -> None:
    """Append MANDATES_FRAGMENT to each tool's __doc__.

    Idempotent: re-importing the module will not double-attach.
    """
    for fn in funcs:
        doc = getattr(fn, "__doc__", None) or ""
        if "LLM BEHAVIOURAL MANDATES" in doc:
            continue
        try:
            fn.__doc__ = doc.rstrip() + "\n" + MANDATES_FRAGMENT  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            # Some tool wrappers (FastMCP) may not allow __doc__ writes; fall
            # back to leaving the docstring as-is. Tests assert on either the
            # wrapper's __doc__ or the wrapped function's __doc__.
            pass
