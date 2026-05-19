"""Regression tests for the LLM-behavioural mandate wiring.

Two mandates are attached to tool docstrings at import time:

  * DESTRUCTIVE — appended by ``mcp_tools._destructive._attach_destructive``
    to every tool whose effect cannot be undone by a follow-up call. Source
    of truth: ``mcp_tools._destructive.DESTRUCTIVE_TOOL_NAMES``.

  * VISUALISATION — appended by
    ``mcp_tools.workspaces._mandates._attach_visualisation`` to the two
    tools that place / reconfigure a parameterised dashboard module. Source
    of truth: ``mcp_tools.workspaces._mandates.VISUALISATION_MANDATE_TOOL_NAMES``.

Attachment is fragile — it lives in module-level code that the tool's file
must call. These tests fail the moment a name in the canonical set is no
longer registered as a tool, or a registered tool in the set is missing
the mandate fragment in its docstring.
"""

from mcp_tools import mcp
from mcp_tools import batch as _batch_module  # noqa: F401 — registers every @mcp.tool
from mcp_tools._destructive import (
    DESTRUCTIVE_FRAGMENT,
    DESTRUCTIVE_TOOL_NAMES,
)
from mcp_tools.workspaces._mandates import (
    VISUALISATION_MANDATE_FRAGMENT,
    VISUALISATION_MANDATE_TOOL_NAMES,
)


def _tool_by_name() -> dict[str, object]:
    return {t.name: t for t in mcp._tool_manager.list_tools()}


def _docstring(tool) -> str:
    """Read the docstring off the FastMCP Tool, falling back to the wrapped
    function. FastMCP copies the docstring on registration; the wrapped fn
    is the canonical source."""
    direct = (getattr(tool, "description", None) or "")
    wrapped = (getattr(tool.fn, "__doc__", None) or "") if hasattr(tool, "fn") else ""
    return direct + "\n" + wrapped


class TestDestructiveMandateWiring:
    """Every name in DESTRUCTIVE_TOOL_NAMES must (a) be a registered tool
    and (b) carry the destructive fragment in its docstring."""

    def test_every_name_is_registered(self):
        registered = set(_tool_by_name())
        missing = DESTRUCTIVE_TOOL_NAMES - registered
        assert not missing, (
            f"DESTRUCTIVE_TOOL_NAMES references tool(s) not registered "
            f"on the MCP server: {sorted(missing)}. Either register them "
            f"or remove them from the canonical set."
        )

    def test_every_destructive_tool_carries_the_mandate(self):
        tools = _tool_by_name()
        # The DESTRUCTIVE fragment contains a distinctive sentinel — the
        # attachment helper writes the full block, but ``MANDATORY
        # DESTRUCTIVE-ACTION CONFIRMATION`` is the unique header we
        # check for.
        sentinel = "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION"
        # Sanity: the fragment we attach actually contains the sentinel.
        assert sentinel in DESTRUCTIVE_FRAGMENT
        missing_mandate: list[str] = []
        for name in DESTRUCTIVE_TOOL_NAMES:
            doc = _docstring(tools[name])
            if sentinel not in doc:
                missing_mandate.append(name)
        assert not missing_mandate, (
            f"Destructive tools missing the destructive mandate in "
            f"their docstring (did the tool file forget to call "
            f"_attach_destructive?): {sorted(missing_mandate)}"
        )

    def test_no_extra_tools_silently_carry_the_destructive_mandate(self):
        """If a tool's docstring contains the destructive sentinel, it
        MUST be in DESTRUCTIVE_TOOL_NAMES. Catches the inverse drift:
        someone attaches the fragment to a new tool but forgets to
        update the canonical set."""
        tools = _tool_by_name()
        sentinel = "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION"
        marked = {
            name for name, tool in tools.items() if sentinel in _docstring(tool)
        }
        extra = marked - DESTRUCTIVE_TOOL_NAMES
        assert not extra, (
            f"Tools carry the destructive mandate but are not listed in "
            f"DESTRUCTIVE_TOOL_NAMES: {sorted(extra)}"
        )

    def test_prose_destructive_rule_lists_every_name(self):
        """The DESTRUCTIVE-ACTION RULE prose in the FastMCP instructions
        must enumerate every name in the canonical set — that prose is
        what the LLM reads at session start."""
        instructions = mcp.instructions or ""
        for name in DESTRUCTIVE_TOOL_NAMES:
            assert name in instructions, (
                f"Destructive tool {name!r} is not enumerated in the "
                f"FastMCP instructions DESTRUCTIVE-ACTION RULE section."
            )


class TestVisualisationMandateWiring:
    """Every name in VISUALISATION_MANDATE_TOOL_NAMES must (a) be a
    registered tool and (b) carry the visualisation fragment."""

    def test_every_name_is_registered(self):
        registered = set(_tool_by_name())
        missing = VISUALISATION_MANDATE_TOOL_NAMES - registered
        assert not missing, (
            f"VISUALISATION_MANDATE_TOOL_NAMES references tool(s) not "
            f"registered on the MCP server: {sorted(missing)}"
        )

    def test_every_visualisation_tool_carries_the_mandate(self):
        tools = _tool_by_name()
        sentinel = "LLM BEHAVIOURAL MANDATES — VISUALISATION"
        assert sentinel in VISUALISATION_MANDATE_FRAGMENT
        missing_mandate: list[str] = []
        for name in VISUALISATION_MANDATE_TOOL_NAMES:
            doc = _docstring(tools[name])
            if sentinel not in doc:
                missing_mandate.append(name)
        assert not missing_mandate, (
            f"Visualisation tools missing the visualisation mandate in "
            f"their docstring (did the tool file forget to call "
            f"_attach_visualisation?): {sorted(missing_mandate)}"
        )

    def test_no_extra_tools_silently_carry_the_visualisation_mandate(self):
        """add_text_module / update_text_module / add_plotly_json_module
        deliberately bypass this mandate — their user input IS the body /
        figure. Anyone who attaches the visualisation fragment to a new
        tool MUST update the canonical set first."""
        tools = _tool_by_name()
        sentinel = "LLM BEHAVIOURAL MANDATES — VISUALISATION"
        marked = {
            name for name, tool in tools.items() if sentinel in _docstring(tool)
        }
        extra = marked - VISUALISATION_MANDATE_TOOL_NAMES
        assert not extra, (
            f"Tools carry the visualisation mandate but are not listed "
            f"in VISUALISATION_MANDATE_TOOL_NAMES: {sorted(extra)}"
        )
