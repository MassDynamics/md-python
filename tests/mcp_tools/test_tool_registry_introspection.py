"""Regression tests for the MCP tool surface.

These tests are the single line of defence against drift between

  1. the set of ``@mcp.tool``-decorated functions in src/mcp_tools/**
  2. the dispatch table used by ``batch`` (``_TOOL_REGISTRY``)
  3. the prose ``TOOL CATEGORIES`` enumeration in the FastMCP
     ``instructions=`` string that the LLM relies on

Previously each addition required hand-editing all three; now (1)+(2) are
derived from FastMCP introspection and (3) is asserted to be in sync here.
A new ``@mcp.tool`` declaration that does not surface in batch or in the
prose category list will fail one of these tests.
"""

import re

from mcp_tools import mcp
from mcp_tools.batch import _TOOL_REGISTRY


def _all_registered_tool_names() -> set[str]:
    return {t.name for t in mcp._tool_manager.list_tools()}


def _tool_categories_section() -> str:
    """Extract the ``TOOL CATEGORIES`` block from the FastMCP instructions.

    The block starts at the ``TOOL CATEGORIES`` heading and runs until the
    next top-level heading. Headings in this prose are uppercase phrases
    that start at column 0 — either followed by ``\\n`` or by ``\\u2014``
    (em dash). We stop at the first such heading after the start.
    """
    instructions = mcp.instructions or ""
    match = re.search(
        # Heading line itself, then content, stopping at the next ALL-CAPS
        # phrase (>=2 uppercase words) that begins a line.
        r"TOOL CATEGORIES[^\n]*\n(?P<body>.*?)(?=\n[A-Z][A-Z]+(?:[ -][A-Z]+)+)",
        instructions,
        re.DOTALL,
    )
    assert match is not None, "TOOL CATEGORIES section missing from instructions"
    return match.group("body")


class TestToolRegistryIntrospection:
    """``_TOOL_REGISTRY`` must equal every ``@mcp.tool`` minus ``batch``."""

    def test_registry_matches_introspection_minus_batch(self):
        registered = _all_registered_tool_names()
        # batch itself is declared via @mcp.tool so it shows up in the
        # introspection list, but it is excluded from the dispatch table
        # to keep batch calls non-recursive.
        expected = registered - {"batch"}
        assert set(_TOOL_REGISTRY) == expected, (
            f"_TOOL_REGISTRY drifted from @mcp.tool registrations. "
            f"In MCP but not registry: {expected - set(_TOOL_REGISTRY)}. "
            f"In registry but not MCP: {set(_TOOL_REGISTRY) - expected}."
        )

    def test_registry_excludes_batch(self):
        """``batch`` itself must not be dispatchable via batch — otherwise
        the LLM can nest batch calls arbitrarily deep."""
        assert "batch" not in _TOOL_REGISTRY

    def test_every_registry_entry_is_callable(self):
        for name, fn in _TOOL_REGISTRY.items():
            assert callable(fn), f"_TOOL_REGISTRY[{name!r}] is not callable"


class TestToolCategoriesPromptInSync:
    """The prose TOOL CATEGORIES list in ``instructions=`` must enumerate
    every ``@mcp.tool``. The LLM reads this prose to pick the right tool;
    a missing entry hides the tool from selection."""

    def test_every_registered_tool_is_named_in_categories(self):
        section = _tool_categories_section()
        registered = _all_registered_tool_names()
        # Tools the prose deliberately groups under "Utility" or that are
        # implementation-details (none today). If we ever add such a tool,
        # opt it in explicitly here rather than silently letting the test
        # ignore it.
        explicitly_listed = {
            name for name in registered if re.search(rf"\b{re.escape(name)}\b", section)
        }
        missing = registered - explicitly_listed
        assert not missing, (
            f"Tools registered via @mcp.tool but missing from the prose "
            f"TOOL CATEGORIES section in mcp_tools.__init__: "
            f"{sorted(missing)}"
        )

    def test_no_unknown_tool_names_in_categories(self):
        """The prose categories list must not advertise tools that no
        longer exist. Words like 'batch' or 'health_check' are tools;
        anything else word-boundary-matching ``[a-z_]+`` in the section
        that looks like a tool name and is not registered is suspicious.
        """
        section = _tool_categories_section()
        registered = _all_registered_tool_names()
        # Heuristic: snake_case tokens of length >= 4 that are mentioned
        # outside the leading line/header but do not match any known
        # tool. We allow generic category words like "tools", "default",
        # "utility" by maintaining a small allowlist.
        allow = {
            "tool",
            "tools",
            "utility",
            "categories",
            "preferred",
            "for",
            "local",
            "files",
            "and",
            "the",
            "order",
            "roughly",
            "in",
            "this",
            "use",
            "file",
            "upload",
            "dataset",
            "pipeline",
            "visualise",
        }
        candidate = set(re.findall(r"\b[a-z_]{4,}\b", section)) - allow
        unknown_looking = {
            c
            for c in candidate
            if "_" in c and c not in registered and not c.startswith("md_")
        }
        assert not unknown_looking, (
            f"Suspicious snake_case names in TOOL CATEGORIES that do not "
            f"map to a registered @mcp.tool: {sorted(unknown_looking)}"
        )
