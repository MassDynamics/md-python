#!/usr/bin/env python3
"""
Generate CLI documentation from the Click command tree.

Usage:
    python scripts/build-docs.py [--output docs/commands-generated.md]

Reads the Click command tree from md_cli.main and generates a markdown
reference. Run this after changing CLI commands to keep docs in sync.
"""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Generate CLI docs")
    parser.add_argument("--output", "-o", type=Path,
                        default=Path("docs/commands-generated.md"))
    args = parser.parse_args()

    try:
        from md_cli.main import cli
        import click
    except ImportError:
        print("Error: md-cli not installed. Run: pip install -e ./cli")
        return

    lines = ["# CLI Command Reference (Auto-Generated)\n"]

    def document_group(group, prefix="md"):
        """Recursively document Click commands."""
        if isinstance(group, click.Group):
            for name in sorted(group.commands):
                cmd = group.commands[name]
                full = f"{prefix} {name}"
                lines.append(f"\n## `{full}`\n")
                if cmd.help:
                    lines.append(f"{cmd.help.strip()}\n")
                if isinstance(cmd, click.Group):
                    document_group(cmd, full)
                else:
                    # Document params
                    for param in cmd.params:
                        if isinstance(param, click.Option):
                            opts = "/".join(param.opts)
                            help_text = param.help or ""
                            default = f" (default: {param.default})" if param.default is not None else ""
                            lines.append(f"- `{opts}`: {help_text}{default}")
                        elif isinstance(param, click.Argument):
                            lines.append(f"- `{param.name}` (required)")
                    lines.append("")
        elif isinstance(group, click.Command):
            if group.help:
                lines.append(f"{group.help.strip()}\n")

    document_group(cli)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines))
    print(f"Generated {args.output}")


if __name__ == "__main__":
    main()
