#!/usr/bin/env python3
"""MCP coverage driver.

Single entry point for the mcp-coverage skill. Subcommands:

  status                          Show pinned_sha vs HEAD for every source.
  refresh --source NAME           Extract one source at HEAD, diff vs manifest.
  refresh --all                   Refresh every source.
  bootstrap --source NAME         Initialise a manifest for the first time.
  commit --source NAME            Bump pinned_sha to HEAD (after human review).

The script reads JSON manifests from ../manifests/ and writes delta reports to
../manifests/.delta-<source>.json. It never mutates committed manifests
implicitly — only `commit` and `bootstrap` write to them.

Stdlib only. No PyYAML, no requests. Designed to run in any Python env.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

SKILL_DIR = Path(__file__).resolve().parent.parent
MANIFESTS_DIR = SKILL_DIR / "manifests"
MD_PYTHON_ROOT = SKILL_DIR.parent.parent.parent  # .../md-python
REPOS_ROOT = MD_PYTHON_ROOT.parent  # .../md-repos


@dataclass
class Source:
    name: str
    repo_path: Path
    extractor: Callable[[Path], list[dict[str, Any]]]
    unit_of_work: str
    scope_paths: list[str] = field(default_factory=list)
    """Repo-relative paths used for `git log <pinned>..HEAD -- <paths>` to scope
    semantic-change hints to the surface this source owns."""


# ---------------------------------------------------------------------------
# Per-source extractors. Each returns a list of dicts; the `id` field is the
# stable unit identifier used for diffing.
# ---------------------------------------------------------------------------


def extract_mcp_tools(repo_path: Path) -> list[dict[str, Any]]:
    """AST-walk src/mcp_tools/ for @mcp.tool()-decorated functions."""
    tools: list[dict[str, Any]] = []
    root = repo_path / "src" / "mcp_tools"
    if not root.exists():
        raise FileNotFoundError(f"{root} does not exist")

    for py_file in sorted(root.rglob("*.py")):
        if py_file.name.startswith("_") or py_file.name == "__init__.py":
            continue
        rel = py_file.relative_to(repo_path)
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            print(f"!! SyntaxError in {rel}: {exc}", file=sys.stderr)
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            if not _is_mcp_tool_decorated(node):
                continue
            docstring = ast.get_docstring(node) or ""
            summary = docstring.strip().split("\n", 1)[0][:200]
            tools.append(
                {
                    "id": node.name,
                    "name": node.name,
                    "file": str(rel),
                    "line": node.lineno,
                    "summary": summary,
                    "resource_calls": _collect_client_calls(node),
                }
            )
    tools.sort(key=lambda t: t["id"])
    return tools


def _is_mcp_tool_decorated(fn: ast.FunctionDef) -> bool:
    for dec in fn.decorator_list:
        # @mcp.tool() or @mcp.tool
        target = dec.func if isinstance(dec, ast.Call) else dec
        if (
            isinstance(target, ast.Attribute)
            and target.attr == "tool"
            and isinstance(target.value, ast.Name)
            and target.value.id == "mcp"
        ):
            return True
    return False


def _collect_client_calls(fn: ast.FunctionDef) -> list[str]:
    """Best-effort: list client.<resource>.<method> call chains inside the
    function body. Used to link MCP tools to md-python resource methods.

    Accepts three call-site shapes used in src/mcp_tools/:
      1. ``client.<resource>...`` after a local ``client = get_client()``
      2. ``get_client().<resource>...`` chained inline
      3. ``self._client.<resource>...`` (rare here, common in resources)
    """
    seen: list[str] = []
    # Detect local rebinds of get_client() so `client.workspaces.foo()` resolves.
    local_aliases: set[str] = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if (
                    isinstance(tgt, ast.Name)
                    and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == "get_client"
                ):
                    local_aliases.add(tgt.id)

    accept = {"client", "_client", "get_client"} | local_aliases
    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            chain = _attr_chain(node.func)
            if chain and chain[0] in accept and len(chain) >= 3:
                rendered = ".".join(chain)
                if rendered not in seen:
                    seen.append(rendered)
    return seen


def _attr_chain(node: ast.AST) -> list[str]:
    out: list[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        out.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        out.append(cur.id)
    elif isinstance(cur, ast.Call):
        # e.g. self._client().workspaces.create  → record the call's func chain
        inner = _attr_chain(cur.func)
        if inner:
            out.extend(inner)
        else:
            return []
    else:
        return []
    return list(reversed(out))


def extract_md_python_resources(repo_path: Path) -> list[dict[str, Any]]:
    """AST-walk src/md_python/resources/ for public methods on resource classes."""
    out: list[dict[str, Any]] = []
    root = repo_path / "src" / "md_python" / "resources"
    if not root.exists():
        raise FileNotFoundError(f"{root} does not exist")

    for py_file in sorted(root.rglob("*.py")):
        if py_file.name == "__init__.py":
            continue
        rel = py_file.relative_to(repo_path)
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            print(f"!! SyntaxError in {rel}: {exc}", file=sys.stderr)
            continue

        # Namespace by sub-package (e.g. ``v2/Datasets.create``) so v1 and v2
        # classes with the same name don't collide.
        ns_parts = rel.relative_to(Path("src/md_python/resources")).parent.parts
        ns = "/".join(ns_parts) + "/" if ns_parts else ""
        for cls in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
            for method in [n for n in cls.body if isinstance(n, ast.FunctionDef)]:
                if method.name.startswith("_"):
                    continue
                out.append(
                    {
                        "id": f"{ns}{cls.name}.{method.name}",
                        "class": cls.name,
                        "method": method.name,
                        "file": str(rel),
                        "line": method.lineno,
                        "signature": _render_signature(method),
                        "http_calls": _collect_http_calls(method),
                    }
                )
    out.sort(key=lambda r: r["id"])
    return out


def _render_signature(fn: ast.FunctionDef) -> str:
    args = []
    for arg in fn.args.args:
        rendered = arg.arg
        if arg.annotation:
            try:
                rendered += f": {ast.unparse(arg.annotation)}"
            except Exception:
                pass
        args.append(rendered)
    ret = ""
    if fn.returns:
        try:
            ret = f" -> {ast.unparse(fn.returns)}"
        except Exception:
            pass
    return f"{fn.name}({', '.join(args)}){ret}"


_HTTP_VERBS = {"get", "post", "put", "patch", "delete"}


def _collect_http_calls(fn: ast.FunctionDef) -> list[dict[str, Any]]:
    """Find HTTP calls inside a resource method.

    md-python resources call the API in two shapes:

    1. ``self._client._make_request(method='POST', url='/api/...', ...)`` —
       the canonical shape under ``src/md_python/resources/v2/``.
    2. ``self._client.<verb>(<url>, ...)`` — convenience shape that the older
       resources use.
    """
    found: list[dict[str, Any]] = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        attr = node.func.attr

        # Shape 1: _make_request(method=..., endpoint=...) — used by v2
        # resources. Accepts ``url=`` as an alias for older code.
        if attr == "_make_request":
            kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
            verb = _kw_constant(kwargs.get("method")) or "?"
            target_node = kwargs.get("endpoint") or kwargs.get("url")
            url = _kw_constant(target_node) or _render_value(target_node)
            if url is None and node.args:
                url = _render_value(node.args[0])
            found.append({"verb": str(verb).upper(), "url": url})
            continue

        # Shape 2: <client>.<verb>(<url>, ...)
        if attr in _HTTP_VERBS:
            chain = _attr_chain(node.func.value)
            if not chain or chain[-1] not in {"_client", "client"}:
                continue
            url = _render_value(node.args[0]) if node.args else None
            found.append({"verb": attr.upper(), "url": url})
    return found


def _kw_constant(value: ast.AST | None) -> str | None:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _render_value(value: ast.AST | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    try:
        return ast.unparse(value)
    except Exception:
        return "<dynamic>"


def extract_workflow_routes(repo_path: Path) -> list[dict[str, Any]]:
    """Prefer `bin/rails routes --json` (need Bundler env); fall back to a
    regex pass over config/routes.rb that only captures `api` namespace entries."""
    routes_rb = repo_path / "config" / "routes.rb"
    if not routes_rb.exists():
        raise FileNotFoundError(f"{routes_rb} does not exist")

    # Try the canonical Rails route dump first
    rails_bin = repo_path / "bin" / "rails"
    if rails_bin.exists():
        try:
            result = subprocess.run(
                [str(rails_bin), "routes", "--json"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout)
                return _filter_api_routes(data)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
            pass

    # Regex fallback: only captures top-level api scope + explicit verb routes.
    # Reports `parsed: false` for blocks it can't follow so the user knows.
    return _scan_routes_rb(routes_rb)


_NON_API_HINTS = (
    "/admins/",
    "/users/auth/",
    "/users/sign",
    "/users/password",
    "/users/invitation",
    "/users/confirmation",
)


def _filter_api_routes(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter the canonical `bin/rails routes --json` output down to MCP-relevant
    endpoints. The workflow app does not namespace its API under `/api/`, so we
    keep everything except devise/admin/asset noise."""
    out = []
    for r in raw:
        path = r.get("path", "")
        if not path or any(h in path for h in _NON_API_HINTS):
            continue
        verb = (r.get("verb") or r.get("method") or "").upper()
        if verb in {"", "HEAD"}:
            continue
        controller = r.get("reqs") or r.get("controller", "")
        out.append(
            {
                "id": f"{verb} {path}",
                "verb": verb,
                "path": path,
                "controller": controller,
                "source": "rails-routes",
            }
        )
    out.sort(key=lambda r: r["id"])
    return out


_VERB_LINE = re.compile(
    r"^\s*(get|post|put|patch|delete)\s+['\"]([^'\"]+)['\"]"
    r"(?:.*?to:\s*['\"]([^'\"]+)['\"])?"
)
_RESOURCES_LINE = re.compile(r"^\s*(resources?)\s+:([a-z_][a-z0-9_]*)")
_NAMESPACE_LINE = re.compile(r"^\s*namespace\s+:([a-z_][a-z0-9_]*)")
_SCOPE_LINE = re.compile(r"^\s*scope\s+['\"]([^'\"]+)['\"]")


def _scan_routes_rb(routes_rb: Path) -> list[dict[str, Any]]:
    """Declaration-level scan of config/routes.rb.

    Emits one unit per ``resources``, ``resource``, ``namespace``, or explicit
    verb declaration, with file:line. Tracks current namespace/scope via a
    simple indent-based stack so nested declarations carry their prefix.

    This is intentionally less precise than ``bin/rails routes --json`` — it
    captures **what was declared**, not the full set of generated paths. That
    is enough to surface "what's new" upstream; for full path-level diffs, run
    the canonical Rails route dump.
    """
    out: list[dict[str, Any]] = []
    stack: list[tuple[int, str]] = []  # (indent, prefix)
    for lineno, raw_line in enumerate(
        routes_rb.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        while stack and stack[-1][0] >= indent:
            stack.pop()
        prefix = stack[-1][1] if stack else ""

        m = _NAMESPACE_LINE.match(raw_line)
        if m:
            new_prefix = f"{prefix}/{m.group(1)}".replace("//", "/")
            stack.append((indent, new_prefix))
            out.append(
                {
                    "id": f"namespace {new_prefix}",
                    "kind": "namespace",
                    "name": m.group(1),
                    "prefix": new_prefix,
                    "line": lineno,
                }
            )
            continue

        m = _SCOPE_LINE.match(raw_line)
        if m:
            new_prefix = f"{prefix}/{m.group(1).strip('/')}".replace("//", "/")
            stack.append((indent, new_prefix))
            continue

        m = _RESOURCES_LINE.match(raw_line)
        if m:
            kind = m.group(1)  # "resources" or "resource"
            name = m.group(2)
            path = f"{prefix}/{name}".replace("//", "/") if prefix else f"/{name}"
            unit_id = f"{kind} {path}"
            if any(h in path for h in _NON_API_HINTS):
                continue
            out.append(
                {
                    "id": unit_id,
                    "kind": kind,
                    "name": name,
                    "path": path,
                    "line": lineno,
                }
            )
            continue

        m = _VERB_LINE.match(raw_line)
        if m:
            verb = m.group(1).upper()
            path = m.group(2)
            full = (
                path if path.startswith("/") else f"{prefix}/{path}".replace("//", "/")
            )
            if any(h in full for h in _NON_API_HINTS):
                continue
            controller = m.group(3) or ""
            out.append(
                {
                    "id": f"{verb} {full}",
                    "kind": "verb",
                    "verb": verb,
                    "path": full,
                    "controller": controller,
                    "line": lineno,
                }
            )

    out.append(
        {
            "id": "__note__",
            "note": (
                "declaration-level scan of config/routes.rb. For full path "
                "expansion (each `resources :x` → 7 CRUD routes) run "
                "`bin/rails routes --json` inside the workflow repo and "
                "rerun this extractor — it will prefer the canonical output."
            ),
        }
    )
    return out


def extract_data_set_service_jobs(repo_path: Path) -> list[dict[str, Any]]:
    """Scan for job_slug evidence — seed files first, migrations second,
    integration-test string literals last."""
    candidates: dict[str, dict[str, Any]] = {}

    # 1. Seed files
    for seed_dir in [repo_path / "db" / "seeds", repo_path / "src" / "db" / "seeds"]:
        if seed_dir.exists():
            for f in seed_dir.rglob("*"):
                if f.is_file():
                    _scan_for_slugs(
                        f, candidates, evidence=f"seed:{f.relative_to(repo_path)}"
                    )

    # 2. Migrations referencing INSERT INTO jobs
    for migr_dir in [repo_path / "alembic", repo_path / "db" / "migrations"]:
        if migr_dir.exists():
            for f in migr_dir.rglob("*.py"):
                if "INTO jobs" in f.read_text(encoding="utf-8", errors="ignore"):
                    _scan_for_slugs(
                        f, candidates, evidence=f"migration:{f.relative_to(repo_path)}"
                    )

    # 3. Test fixtures
    tests_dir = repo_path / "tests"
    if tests_dir.exists():
        for f in tests_dir.rglob("*.py"):
            content = f.read_text(encoding="utf-8", errors="ignore")
            if "job_slug" in content:
                _scan_for_slugs(
                    f, candidates, evidence=f"test:{f.relative_to(repo_path)}"
                )

    out = sorted(candidates.values(), key=lambda r: r["id"])
    if not out:
        out.append(
            {
                "id": "__warning__",
                "warning": (
                    "no slugs found via seeds/migrations/tests — run "
                    "`GET /jobs` against a live data-set-service and import the response"
                ),
            }
        )
    return out


_SLUG_PAT = re.compile(r"['\"]([a-z][a-z0-9-]{2,40})['\"]")
_SLUG_HINTS = {
    "pairwise-comparison",
    "dose-response",
    "normalisation-imputation",
    "anova",
    "qc",
    "lfq",
}


def _scan_for_slugs(f: Path, into: dict[str, dict[str, Any]], evidence: str) -> None:
    text = f.read_text(encoding="utf-8", errors="ignore")
    if "job_slug" not in text and "slug" not in text:
        return
    for match in _SLUG_PAT.findall(text):
        looks_like_slug = "-" in match or match in _SLUG_HINTS
        if not looks_like_slug:
            continue
        entry = into.setdefault(match, {"id": match, "slug": match, "evidence": []})
        if evidence not in entry["evidence"]:
            entry["evidence"].append(evidence)


def extract_visualisations_modules(repo_path: Path) -> list[dict[str, Any]]:
    """Delegate to md-viz-modules' extract.js when available; otherwise scan
    the visualisations-service request classes as a coarse fallback (id list
    only, no parameter fingerprint)."""
    md_viz_extract = (
        Path.home() / ".claude" / "skills" / "md-viz-modules" / "scripts" / "extract.js"
    )
    if md_viz_extract.exists():
        try:
            result = subprocess.run(
                ["node", str(md_viz_extract), "--list-only", "--json"],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout)
                if isinstance(data, list):
                    return sorted(data, key=lambda m: m.get("id", ""))
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
            pass

    # Fallback: list every Request subclass file under the service's request modules.
    requests_root = repo_path / "src"
    out: list[dict[str, Any]] = []
    if requests_root.exists():
        for py in requests_root.rglob("*request*.py"):
            rel = py.relative_to(repo_path)
            out.append({"id": py.stem, "file": str(rel), "source": "fallback"})
    out.append(
        {
            "id": "__warning__",
            "warning": (
                "md-viz-modules extract.js not invoked successfully — the list "
                "above is a coarse filename scan, not the parameter-aware "
                "catalogue. Run md-viz-modules' own extractor for the truth."
            ),
        }
    )
    return out


def extract_entity_mapping_routes(repo_path: Path) -> list[dict[str, Any]]:
    """Grep for FastAPI/Flask route decorators, deduped by id."""
    src = repo_path / "src"
    if not src.exists():
        src = repo_path / "app"
    if not src.exists():
        raise FileNotFoundError(f"no src/ or app/ in {repo_path}")
    pattern = re.compile(
        r"@(?:app|router)\.(get|post|put|patch|delete)\s*\(\s*['\"]([^'\"]+)['\"]"
    )
    seen: dict[str, dict[str, Any]] = {}
    for py in src.rglob("*.py"):
        rel = py.relative_to(repo_path)
        for verb, path in pattern.findall(
            py.read_text(encoding="utf-8", errors="ignore")
        ):
            if path in {"/health", "/metrics", "/"} or path.startswith("/_"):
                continue
            unit_id = f"{verb.upper()} {path}"
            if unit_id in seen:
                # collect supporting files but keep first as canonical
                seen[unit_id].setdefault("seen_in", []).append(str(rel))
                continue
            seen[unit_id] = {
                "id": unit_id,
                "verb": verb.upper(),
                "path": path,
                "file": str(rel),
            }
    return sorted(seen.values(), key=lambda r: r["id"])


# Top-level subpackages of ``md-converter/src/mdconverter/`` that are not
# format readers — listed here so the extractor doesn't enumerate them.
_MD_CONVERTER_NON_FORMAT = {
    "__pycache__",
    "execute",
    "executor",
    "parameters",
    "utils",
    "unknown",
    "complete",
    "prepare",
    "qc",
}


def extract_md_converter_formats(repo_path: Path) -> list[dict[str, Any]]:
    """List immediate subpackages of ``src/mdconverter/`` that aren't utility
    modules — each represents a supported input format."""
    base = repo_path / "src" / "mdconverter"
    if not base.exists():
        raise FileNotFoundError(f"{base} does not exist")
    out: list[dict[str, Any]] = []
    for child in sorted(base.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith((".", "_")) or child.name in _MD_CONVERTER_NON_FORMAT:
            continue
        out.append(
            {
                "id": child.name,
                "format": child.name,
                "package_path": str(child.relative_to(repo_path)),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------


def _sources() -> dict[str, Source]:
    return {
        "mcp_tools": Source(
            name="mcp_tools",
            repo_path=MD_PYTHON_ROOT,
            extractor=extract_mcp_tools,
            unit_of_work="mcp_tool",
            scope_paths=["src/mcp_tools"],
        ),
        "md_python_resources": Source(
            name="md_python_resources",
            repo_path=MD_PYTHON_ROOT,
            extractor=extract_md_python_resources,
            unit_of_work="resource_method",
            scope_paths=["src/md_python/resources"],
        ),
        "workflow": Source(
            name="workflow",
            repo_path=REPOS_ROOT / "workflow",
            extractor=extract_workflow_routes,
            unit_of_work="api_endpoint",
            scope_paths=["config/routes.rb", "app/controllers/api"],
        ),
        "data_set_service": Source(
            name="data_set_service",
            repo_path=REPOS_ROOT / "data-set-service",
            extractor=extract_data_set_service_jobs,
            unit_of_work="job_slug",
            scope_paths=["db", "src/db", "alembic", "src/routes/jobs.py"],
        ),
        "visualisations_service": Source(
            name="visualisations_service",
            repo_path=REPOS_ROOT / "visualisations-service",
            extractor=extract_visualisations_modules,
            unit_of_work="module_type",
            scope_paths=["src"],
        ),
        "entity_mapping_service": Source(
            name="entity_mapping_service",
            repo_path=REPOS_ROOT / "entity-mapping-service",
            extractor=extract_entity_mapping_routes,
            unit_of_work="api_endpoint",
            scope_paths=["src", "app"],
        ),
        "md_converter": Source(
            name="md_converter",
            repo_path=REPOS_ROOT / "md-converter",
            extractor=extract_md_converter_formats,
            unit_of_work="format",
            scope_paths=["."],
        ),
    }


# ---------------------------------------------------------------------------
# Manifest IO
# ---------------------------------------------------------------------------


def _manifest_path(name: str) -> Path:
    return MANIFESTS_DIR / f"{name}.json"


def _delta_path(name: str) -> Path:
    return MANIFESTS_DIR / f".delta-{name}.json"


def _load_manifest(name: str) -> dict[str, Any]:
    path = _manifest_path(name)
    if not path.exists():
        return {
            "source": name,
            "pinned_sha": None,
            "coverage": [],
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _save_manifest(name: str, manifest: dict[str, Any]) -> None:
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    _manifest_path(name).write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _git_head(repo: Path) -> str | None:
    if not (repo / ".git").exists():
        return None
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        return out.stdout.strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def _git_log_scope(repo: Path, since: str, paths: list[str]) -> list[str]:
    """Return one-line summaries of commits in `since..HEAD` touching `paths`."""
    try:
        out = subprocess.run(
            ["git", "log", "--oneline", f"{since}..HEAD", "--", *paths],
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []
    return [line for line in out.stdout.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def _diff(manifest: dict[str, Any], live: list[dict[str, Any]]) -> dict[str, Any]:
    pinned = {entry["id"]: entry for entry in manifest.get("coverage", [])}
    live_map = {entry["id"]: entry for entry in live if "id" in entry}

    added = [live_map[k] for k in live_map.keys() - pinned.keys()]
    removed = [pinned[k] for k in pinned.keys() - live_map.keys()]
    changed = []
    for k in pinned.keys() & live_map.keys():
        old = {
            kk: vv
            for kk, vv in pinned[k].items()
            if kk not in {"mcp_tool", "status", "notes"}
        }
        new = {kk: vv for kk, vv in live_map[k].items()}
        if old != new:
            changed.append({"id": k, "before": old, "after": new})

    return {
        "added": sorted(added, key=lambda r: r.get("id", "")),
        "removed": sorted(removed, key=lambda r: r.get("id", "")),
        "changed": sorted(changed, key=lambda r: r.get("id", "")),
    }


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_status(_: argparse.Namespace) -> int:
    print(f"{'source':<28} {'pinned':<14} {'head':<14} status")
    for name, src in _sources().items():
        manifest = _load_manifest(name)
        pinned = (manifest.get("pinned_sha") or "—")[:12]
        head = (_git_head(src.repo_path) or "—")[:12]
        if pinned == head:
            status = "in sync"
        elif manifest.get("pinned_sha") is None:
            status = "not bootstrapped"
        else:
            status = "DRIFTED"
        print(f"{name:<28} {pinned:<14} {head:<14} {status}")
    return 0


def cmd_refresh(args: argparse.Namespace) -> int:
    targets = list(_sources().keys()) if args.all else [args.source]
    if not targets or any(t is None for t in targets):
        print("error: --source NAME or --all required", file=sys.stderr)
        return 2

    for name in targets:
        src = _sources().get(name)
        if not src:
            print(f"!! unknown source: {name}", file=sys.stderr)
            continue
        print(f"\n=== {name} ===")
        manifest = _load_manifest(name)
        pinned = manifest.get("pinned_sha")
        head = _git_head(src.repo_path)
        print(f"repo:   {src.repo_path}")
        print(f"pinned: {pinned or '(none — needs bootstrap)'}")
        print(f"head:   {head or '(not a git repo)'}")

        if pinned and head and pinned == head:
            print("→ in sync, skipping extraction")
            continue
        if not src.repo_path.exists():
            print(f"!! repo path missing: {src.repo_path}", file=sys.stderr)
            continue

        try:
            live = src.extractor(src.repo_path)
        except Exception as exc:
            print(f"!! extractor failed: {exc}", file=sys.stderr)
            continue

        delta = _diff(manifest, live)
        commits = (
            _git_log_scope(src.repo_path, pinned, src.scope_paths)
            if pinned and head
            else []
        )

        report = {
            "source": name,
            "pinned_sha": pinned,
            "head_sha": head,
            "scope_paths": src.scope_paths,
            "diff": delta,
            "commits_in_scope": commits,
            "live_unit_count": sum(
                1 for u in live if not u.get("id", "").startswith("__")
            ),
        }
        _delta_path(name).write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )

        n_add = len(delta["added"])
        n_rm = len(delta["removed"])
        n_ch = len(delta["changed"])
        print(
            f"→ delta: +{n_add} new, -{n_rm} removed, ~{n_ch} changed "
            f"(report: {_delta_path(name).relative_to(SKILL_DIR)})"
        )
        for entry in delta["added"][:10]:
            print(f"   + {entry.get('id')}")
        if n_add > 10:
            print(f"   … and {n_add - 10} more")
        for entry in delta["removed"][:5]:
            print(f"   - {entry.get('id')}")
        if commits:
            print(f"   commits in scope ({len(commits)}):")
            for line in commits[:5]:
                print(f"     {line}")
            if len(commits) > 5:
                print(f"     … and {len(commits) - 5} more")
    return 0


def cmd_bootstrap(args: argparse.Namespace) -> int:
    src = _sources().get(args.source)
    if not src:
        print(f"unknown source: {args.source}", file=sys.stderr)
        return 2
    if not src.repo_path.exists():
        print(f"repo missing: {src.repo_path}", file=sys.stderr)
        return 1
    live = src.extractor(src.repo_path)
    head = _git_head(src.repo_path)
    manifest = {
        "source": args.source,
        "repo_path": str(src.repo_path),
        "unit_of_work": src.unit_of_work,
        "pinned_sha": head,
        "scope_paths": src.scope_paths,
        "coverage": [
            {**entry, "mcp_tool": None, "status": "unreviewed"}
            for entry in live
            if not entry.get("id", "").startswith("__")
        ],
    }
    _save_manifest(args.source, manifest)
    print(f"wrote {_manifest_path(args.source)}")
    print(f"  {len(manifest['coverage'])} units pinned at {head}")
    print(
        "next: open the manifest, annotate `mcp_tool` and `status` for each entry, "
        "and commit it."
    )
    return 0


def cmd_commit(args: argparse.Namespace) -> int:
    src = _sources().get(args.source)
    if not src:
        print(f"unknown source: {args.source}", file=sys.stderr)
        return 2
    manifest = _load_manifest(args.source)
    head = _git_head(src.repo_path)
    if not head:
        print(f"!! cannot read git HEAD of {src.repo_path}", file=sys.stderr)
        return 1
    manifest["pinned_sha"] = head
    _save_manifest(args.source, manifest)
    delta = _delta_path(args.source)
    if delta.exists():
        delta.unlink()
    print(f"bumped {args.source} pinned_sha → {head}")
    return 0


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="coverage.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="show pinned vs HEAD per source").set_defaults(
        func=cmd_status
    )

    refresh = sub.add_parser("refresh", help="extract and diff against manifest")
    group = refresh.add_mutually_exclusive_group(required=True)
    group.add_argument("--source", help="source name (see status)")
    group.add_argument("--all", action="store_true", help="refresh every source")
    refresh.set_defaults(func=cmd_refresh)

    boot = sub.add_parser("bootstrap", help="initialise manifest for a source")
    boot.add_argument("--source", required=True)
    boot.set_defaults(func=cmd_bootstrap)

    commit = sub.add_parser("commit", help="bump pinned_sha to HEAD (after review)")
    commit.add_argument("--source", required=True)
    commit.set_defaults(func=cmd_commit)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
