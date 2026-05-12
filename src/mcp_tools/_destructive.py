"""Shared LLM-behaviour mandate for destructive MCP tools.

A destructive tool is one whose effect cannot be undone by a follow-up call.
For this MCP that means:

  * delete_upload          — wipes upload + uploaded files (server returns 204).
  * delete_dataset         — wipes a pipeline result dataset (server returns 204).
  * cancel_dataset         — irrevocably halts a running pipeline job.
  * cancel_upload_queue    — drops queued background file transfers; uploads
                             that hadn't started never will (their server
                             records remain in PENDING).
  * update_sample_metadata — REPLACES the entire sample_metadata array on an
                             upload; there is no cell-level patch API.

All five carry the same agent-side behavioural mandate:

  1. Echo every target id back to the user in plain prose.
  2. Describe the consequence in one sentence (e.g. "deletes the dataset and
     any downstream pairwise / ANOVA results that consumed it").
  3. Wait for an EXPLICIT confirmation token from the user — for example
     "yes, delete <id>" or "yes, cancel <id>". Silence is NOT confirmation.
     A generic "OK" or "go ahead" is not enough; the user must echo the
     action verb and the id back.
  4. Never chain a destructive call after a query in the same turn — give
     the user a chance to react to the query first.

The fragment is attached to each destructive tool's docstring at import time
via ``_attach_destructive`` (mirrors ``mcp_tools.pipelines._mandates._attach``).
Tests assert the fragment is present, so future tools that get added to the
destructive set will fail loudly if the maintainer forgets to attach it.
"""

# Per-tool mandate fragment — append to every destructive tool docstring.
DESTRUCTIVE_FRAGMENT = """

══════════════════════════════════════════════════════════════════════════════
LLM BEHAVIOURAL MANDATE — DESTRUCTIVE ACTION (binding)
══════════════════════════════════════════════════════════════════════════════

MANDATORY DESTRUCTIVE-ACTION CONFIRMATION.
Before calling this tool, the LLM MUST:

  1. Echo every target id (upload_id / dataset_id / etc.) back to the user
     verbatim, in prose. If multiple ids would be affected, list them all.
  2. State the consequence in plain language — for example:
       * delete_upload          → "permanently deletes the upload, its files,
                                   and any datasets attached to it. Cannot
                                   be undone."
       * delete_dataset         → "permanently deletes this dataset and any
                                   downstream pairwise / ANOVA / dose-response
                                   results computed from it."
       * cancel_dataset         → "stops the running pipeline job. The dataset
                                   record stays but its results are gone."
       * cancel_upload_queue    → "discards every queued background file
                                   transfer. Uploads that had not started
                                   uploading will never start; their records
                                   will remain in PENDING."
       * update_sample_metadata → "REPLACES the whole sample_metadata array;
                                   any downstream NI / pairwise / ANOVA /
                                   dose-response datasets become analytically
                                   stale and should be re-run or deleted."
  3. Wait for an EXPLICIT confirmation that names the verb and the id —
     e.g. "yes, delete <id>", "yes, cancel <id>", "yes, overwrite <id>".
     Silence is NOT confirmation. A bare "OK" / "go ahead" is NOT enough.
  4. NEVER chain this destructive call after a query in the same turn. The
     user must see the query result and confirm before the destructive call.
══════════════════════════════════════════════════════════════════════════════
"""


def _attach_destructive(*funcs: object) -> None:
    """Append DESTRUCTIVE_FRAGMENT to each destructive tool's __doc__.

    Idempotent: re-importing the module will not double-attach. Mirrors the
    pattern in ``mcp_tools.pipelines._mandates._attach``.
    """
    for fn in funcs:
        doc = getattr(fn, "__doc__", None) or ""
        if "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in doc:
            continue
        try:
            fn.__doc__ = doc.rstrip() + "\n" + DESTRUCTIVE_FRAGMENT  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            # Some tool wrappers (FastMCP) may not allow __doc__ writes; fall
            # back to leaving the docstring as-is. Tests assert on either the
            # wrapper's __doc__ or the wrapped function's __doc__.
            pass
