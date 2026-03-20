#!/usr/bin/env python3
"""
Build the .skill package from the skill/ source directory.

Usage:
    python scripts/build-skill.py [--output dist/md-platform.skill]

Packages skill/ into a .skill zip archive (same format as the skill-creator
packager). Also copies the CLI source into the skill's scripts/ directory
so the skill is self-contained.
"""

import argparse
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_DIR = REPO_ROOT / "skill"
CLI_DIR = REPO_ROOT / "cli"

EXCLUDE = {".DS_Store", "__pycache__", "*.pyc", ".git"}


def should_exclude(path: Path) -> bool:
    for part in path.parts:
        if part in EXCLUDE:
            return True
    if path.suffix == ".pyc":
        return True
    return False


def build_skill(output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add skill/ contents
        for file in sorted(SKILL_DIR.rglob("*")):
            if file.is_file() and not should_exclude(file.relative_to(SKILL_DIR)):
                arcname = f"md-platform/{file.relative_to(SKILL_DIR)}"
                zf.write(file, arcname)
                print(f"  Added: {arcname}")

        # Bundle CLI source into scripts/md-cli/
        for file in sorted(CLI_DIR.rglob("*")):
            if file.is_file() and not should_exclude(file.relative_to(CLI_DIR)):
                arcname = f"md-platform/scripts/md-cli/{file.relative_to(CLI_DIR)}"
                zf.write(file, arcname)
                print(f"  Added: {arcname}")

    print(f"\n✅ Packaged to {output_path} ({output_path.stat().st_size:,} bytes)")


def main():
    parser = argparse.ArgumentParser(description="Build md-platform.skill")
    parser.add_argument("--output", "-o", type=Path,
                        default=REPO_ROOT / "dist" / "md-platform.skill",
                        help="Output path")
    args = parser.parse_args()
    build_skill(args.output)


if __name__ == "__main__":
    main()
