---
name: mcp-coverage
description: >
  Find what's new in the upstream Mass Dynamics codebases (workflow, data-set-service,
  visualisations-service, entity-mapping-service, md-converter) and surface what the
  MCP server doesn't yet wrap — without re-reading every repo from scratch. Use this
  skill whenever the user wants to "see what's new", "find gaps in MCP coverage",
  "what should we add to the MCP next", "diff the API against the MCP", or before
  dispatching agents to extend the MCP server. Drives off committed JSON manifests
  with pinned upstream SHAs, so each refresh is a mechanical diff instead of an
  open-ended read.
---

# MCP Coverage Skill

The point of this skill is **speed**. Without it, an agent has to walk every
upstream repo (workflow, data-set-service, visualisations-service, …) and compare
each surface against the MCP tools and md-python resources. That's expensive.

With it, the agent reads **manifests** that pin each upstream repo to a SHA and list
its surface (endpoints, job_slugs, module_types, …). A refresh becomes:

1. ask each upstream repo whether HEAD has moved since the pinned SHA;
2. for repos that moved, run a tiny per-source extractor against HEAD and diff
   against the committed coverage list;
3. emit a *delta report* — new units, removed units, signature changes — annotated
   with which MCP tool (if any) covers each.

If a source's pinned SHA == HEAD, there is **nothing to do for that source**. Do
not re-read its code.

---

## When to use this skill

Use this skill **immediately** when the user asks:

- "what's new upstream" / "what changed in workflow / vis-service / dataset-service"
- "what should we add to the MCP next"
- "find gaps in MCP coverage"
- "diff the API against the MCP"
- "before we start tasking agents to extend the MCP, refresh coverage"
- "what dataset jobs / module types are we missing"

Do **not** use it for visualisation-module parameter lookups (use `md-viz-modules`)
or for driving the MCP itself (use `md-mcp-ops`).

---

## Sources tracked

| Source                       | Local clone                                            | Unit of work       | Manifest                                |
|------------------------------|--------------------------------------------------------|--------------------|-----------------------------------------|
| md-python (this repo)        | `.`                                                    | MCP tool           | `manifests/mcp_tools.json`              |
| md-python client resources   | `.`                                                    | resource method    | `manifests/md_python_resources.json`    |
| workflow (Rails gateway)     | `/Users/giuseppeinfusini/wd/md-repos/workflow`         | API endpoint       | `manifests/workflow.json`               |
| data-set-service             | `/Users/giuseppeinfusini/wd/md-repos/data-set-service` | job_slug           | `manifests/data_set_service.json`       |
| visualisations-service       | `/Users/giuseppeinfusini/wd/md-repos/visualisations-service` | module_type   | `manifests/visualisations_service.json` |
| entity-mapping-service       | `/Users/giuseppeinfusini/wd/md-repos/entity-mapping-service` | endpoint     | `manifests/entity_mapping_service.json` |
| md-converter                 | `/Users/giuseppeinfusini/wd/md-repos/md-converter`     | format / reader    | `manifests/md_converter.json`           |

The visualisations-service manifest cross-references the existing `md-viz-modules`
skill — that skill already owns the per-module catalogue (`extract.js`). The
coverage skill diffs the *catalogue size and module-id list* against the MCP's
`list_module_types` surface; it does **not** duplicate per-parameter extraction.

---

## How to run a refresh

Activate the project's conda env first (mandatory — see `feedback_venv.md` in memory):

```bash
conda activate md-api-python
cd /Users/giuseppeinfusini/wd/md-repos/md-python
```

Then run the skill driver:

```bash
# Refresh ONE source — fast, scoped
python .claude/skills/mcp-coverage/scripts/coverage.py refresh --source workflow

# Refresh ALL sources
python .claude/skills/mcp-coverage/scripts/coverage.py refresh --all

# Show the current pinned SHA and HEAD for each source (no extraction)
python .claude/skills/mcp-coverage/scripts/coverage.py status

# Bootstrap a source for the first time (writes the initial manifest)
python .claude/skills/mcp-coverage/scripts/coverage.py bootstrap --source workflow
```

The driver:

1. reads the manifest for the source(s),
2. compares `pinned_sha` to the local clone's HEAD; if equal, prints "no change"
   and exits for that source,
3. otherwise runs the named extractor, diffs against the manifest, and writes a
   delta report to `manifests/.delta-<source>.json`,
4. prints a human summary of new / removed / changed units, with the MCP coverage
   status for each.

**The driver never writes back to the manifest.** Updating `pinned_sha` and the
`coverage:` list is a deliberate human step performed *after* you (Claude) have
reviewed the delta with the user and decided which units are now actually wrapped.
Use `coverage.py commit --source <name>` to bump `pinned_sha` to HEAD only when
the user explicitly approves.

---

## What you (Claude) do with the delta

After the driver prints a delta, your job is to **turn it into a dispatch list**,
not to start implementing. For each new unit:

1. Decide whether it belongs in the MCP at all (admin-only routes, internal
   plumbing, dev tools — skip and mark `status: out_of_scope` when the user
   approves the manifest update).
2. If it does belong, write a one-line task description an agent can execute:
   *"Wrap `POST /api/v2/entity_lists/:id/clone` (workflow controller
   `api/v2/entity_lists_controller#clone`) as MCP tool `clone_entity_list`. Add a
   matching method to `src/md_python/resources/v2/entity_lists.py` first."*
3. Cross-reference the existing MCP/resource manifests to spot prerequisites —
   if the wrapping needs a new md-python resource method, flag that as a
   sub-task.
4. Present the list to the user grouped by *blast radius*: small wraps first,
   then anything touching pipelines or destructive operations (those need
   careful docstring + `_destructive` updates per `src/mcp_tools/_destructive.py`).

Do **not** start coding from the delta in the same turn. Get the user's sign-off
on the dispatch list first.

---

## Per-source recipes

The narrative for each source — where things live, which paths to extract, which
fallbacks to try if the canonical extractor fails — lives in
[`references/recipes.md`](references/recipes.md). Read that page before bootstrap
or before debugging a misbehaving extractor.

---

## When the extractor breaks

Upstream repos restructure. The extractor scripts in `scripts/` are the brittle
part; when one fails:

1. **Do not** fall back to reading the entire upstream repo. That defeats the
   skill's purpose.
2. Update the extractor recipe in `references/recipes.md` and the extractor
   function in `scripts/coverage.py`, then re-run. The fix is almost always a
   path or grep pattern.
3. If a source has moved to a fundamentally different layout, surface it to the
   user as a decision: "extractor needs a rewrite, this is a 1-hour task — do
   you want me to scope it now or skip this source on this pass?"

---

## What this skill is NOT

- **Not a code-reading tool.** It reads manifests, not source.
- **Not the visualisation catalogue.** Use `md-viz-modules` for per-module
  parameter details.
- **Not the MCP runtime.** Use `md-mcp-ops` for executing MCP tools.
- **Not a code generator.** It produces a dispatch list; humans approve, then
  agents implement.
