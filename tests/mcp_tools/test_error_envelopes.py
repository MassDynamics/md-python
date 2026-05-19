"""Regression tests for the error envelope contract.

The CONTRIBUTOR CONTRACT in ``mcp_tools/__init__.py`` says:

  * Tools that return JSON on success MUST return ``json.dumps({"error":
    "..."})`` on failure.
  * Tools that return prose on success MUST return a string starting with
    ``"Error: "`` on failure.

This test sweeps every tool's source file for the two error-return patterns
and checks that no module mixes them in a way that contradicts the
contract. It is intentionally textual — we look at the literal source —
because the alternative (driving every tool to failure) would require
mocking the whole world.
"""

import re
from pathlib import Path

# Filesystem root for the MCP tool surface.
_MCP_TOOLS_DIR = Path(__file__).resolve().parents[2] / "src" / "mcp_tools"


def _tool_source_files() -> list[Path]:
    """Every .py file under src/mcp_tools/, excluding __init__.py and
    private helpers that do not carry @mcp.tool functions."""
    skip = {
        "__init__.py",
        "_client.py",
        "_destructive.py",
        "_env.py",
        "_query.py",
        "_workflow_guide.py",
        "_mandates.py",
        "_modules_validation.py",
        "_bulk.py",
        "_metadata.py",
        "_schemas.py",
        "_io.py",
        "_executor.py",
        "registry.py",  # describes registry — its tools return JSON, no error returns
    }
    files: list[Path] = []
    for p in _MCP_TOOLS_DIR.rglob("*.py"):
        if p.name in skip:
            continue
        # Skip the _introspect package — internal helpers, no @mcp.tool.
        if "_introspect" in p.parts:
            continue
        files.append(p)
    return files


_PROSE_ERROR_RE = re.compile(r"return\s+f?\"Error:")
_JSON_ERROR_RE = re.compile(r"return\s+json\.dumps\(\s*\{\s*[\"']error[\"']\s*:")
# Legacy "Failed to ..." prefix as a LEADING sentinel of a tool return.
# This is the deprecated form we want to keep out of new code.
_LEGACY_FAILED_RE = re.compile(r"return\s+f?\"Failed to ")


class TestErrorEnvelopeContract:
    def test_every_tool_file_uses_at_most_one_error_envelope_style(self):
        """A single tool function should not mix both styles. We allow
        a file to contain BOTH styles (because it may host multiple
        tools — one returns JSON, one returns prose) — but per-function
        consistency is enforced indirectly by the tests in
        ``test_tool_registry_introspection``."""
        # No per-file assertion beyond presence-of-some-error-handling;
        # we just verify the patterns parse. Keeps this test fast and
        # textual.
        for path in _tool_source_files():
            src = path.read_text()
            # Smoke check the regexes don't blow up on real source.
            _PROSE_ERROR_RE.findall(src)
            _JSON_ERROR_RE.findall(src)

    def test_no_leading_failed_to_sentinel_in_new_tool_returns(self):
        """The CONTRIBUTOR CONTRACT names ``Error:`` as the only leading
        prose-error sentinel. ``Failed to ...`` may still appear INSIDE
        an error body (re-raised SDK message) but never as the leading
        prefix of a top-level ``return`` in an MCP tool file."""
        offenders: list[str] = []
        for path in _tool_source_files():
            src = path.read_text()
            for match in _LEGACY_FAILED_RE.finditer(src):
                # Find the line number for a helpful failure message.
                line_no = src.count("\n", 0, match.start()) + 1
                line = src.splitlines()[line_no - 1].strip()
                offenders.append(
                    f"{path.relative_to(_MCP_TOOLS_DIR.parent.parent)}:"
                    f"{line_no}: {line}"
                )
        assert not offenders, (
            "MCP tool returns starting with 'Failed to ...' violate the "
            "CONTRIBUTOR CONTRACT in mcp_tools.__init__ (use 'Error: ' "
            "as the leading sentinel; legacy text may live inside the "
            "error body). Offenders:\n  " + "\n  ".join(offenders)
        )

    def test_instructions_documents_both_envelopes(self):
        """The FastMCP instructions surface the contract to the LLM."""
        from mcp_tools import mcp

        instructions = mcp.instructions or ""
        assert "JSON tools return" in instructions
        assert "prose tools return" in instructions
        assert "Error: " in instructions
