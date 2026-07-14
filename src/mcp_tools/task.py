"""Task-boundary tools — the objective and the outcome.

These two tools exist for ONE reason: the MCP server sees actions, never the
conversation. The protocol gives it no view of what the user asked for or
whether they got it. For Claude Code that can be recovered by joining the local
transcript, but Claude Desktop keeps conversations server-side — there is no
transcript on disk, and most of the team is on Desktop.

So the objective has to be volunteered. ``begin_task`` states it in the model's
own words; ``end_task`` states whether it was achieved. Together they give the
one thing a tool-call log can never infer on its own: **did this actually work?**
Without them, "did the agent achieve the objective" can only be guessed at from
retry patterns — which is exactly how it was being guessed at before.

They are deliberately cheap: two calls per session, no server round-trip, no
side effects. They are pure telemetry markers. When the telemetry plugin is not
installed they are harmless no-ops.
"""

import json

from . import mcp

__all__ = ["begin_task", "end_task"]


@mcp.tool()
def begin_task(objective: str, plan: str = "") -> str:
    """Declare what you are about to do, BEFORE you start doing it.

    Call this ONCE at the start of every distinct user request that will
    involve Mass Dynamics tools — right after you understand what the user
    wants and before your first real MCP call. Call it again if the user
    changes goal mid-session (each call starts a new task segment).

    This records the OBJECTIVE. The MCP server cannot see the conversation, so
    if you do not state the goal, nothing downstream can tell whether the tool
    calls that follow actually achieved it.

    Args:
        objective: what the USER wants, in one or two plain sentences, in your
            own words. Concrete, not generic. Good: "Upload the GSE212702
            transcriptomics counts, normalise with CPM, and build a QC tab."
            Bad: "Help the user with their data."
        plan: optional — the tool sequence you intend to run, one line.

    Returns a confirmation string. Has no side effects on the platform.
    """
    return json.dumps(
        {"status": "task_started", "objective": objective, "plan": plan or None},
        indent=2,
    )


@mcp.tool()
def end_task(outcome: str, achieved: bool, blocked_by: str = "") -> str:
    """Declare whether you achieved the objective. Call this at the END.

    Call this ONCE when the task from ``begin_task`` is finished — whether it
    succeeded, partly succeeded, or failed. **Report honestly.** A task that
    failed is far more useful here than a task falsely marked achieved: the
    failures are what tell us which tools and which skills are broken.

    ``achieved=False`` is the correct answer whenever you did not deliver what
    the user actually asked for — including when you gave up, ran out of a way
    forward, worked around the goal, or delivered something adjacent to it. Do
    not mark a task achieved because the last tool call returned 200.

    Args:
        outcome: what actually happened, in one or two plain sentences.
        achieved: True only if the user got what they asked for.
        blocked_by: when not achieved — the specific thing that stopped you
            (e.g. "ENRICHMENT table names are not enumerable, so the GSEA
            results could not be downloaded"). This is the most valuable field
            in the whole telemetry set; be specific.

    Returns a confirmation string. Has no side effects on the platform.
    """
    return json.dumps(
        {
            "status": "task_ended",
            "achieved": achieved,
            "outcome": outcome,
            "blocked_by": blocked_by or None,
        },
        indent=2,
    )
