# Per-source extraction recipes

This page is the operational guide for each upstream source. Read it before
running `coverage.py bootstrap` or before debugging an extractor.

Each section answers four questions:

1. **What is the unit of work?** (endpoint, job_slug, module_type, …)
2. **Where does it live in the upstream repo?** (canonical paths + fallbacks)
3. **How is it extracted?** (the exact command the driver runs)
4. **What is it cross-referenced against on the MCP side?** (the field the diff
   uses to determine coverage)

---

## md-python — MCP tools (this repo)

- **Unit of work:** `@mcp.tool()`-decorated function in `src/mcp_tools/`.
- **Canonical path:** `src/mcp_tools/**/*.py`. Helpers prefixed with `_` (e.g.
  `_destructive.py`, `_client.py`, `_env.py`, `_query.py`) are excluded.
- **Extraction:** AST walk — find every function whose decorator list contains
  a call to `mcp.tool(...)`. Capture `{name, file, line, docstring_summary,
  resource_calls}`. `resource_calls` is a best-effort grep within the function
  body for `client.<resource>.<method>(...)` patterns (links to the
  md-python resource layer).
- **Cross-ref:** matches `mcp_tool` field on every other manifest.
- **Output:** `manifests/mcp_tools.json`.

The MCP server registers tools at import time via `FastMCP.tool()`. AST parsing
is preferred over runtime introspection because it does not require a running
server and is safe to run in any env.

---

## md-python — client resources (this repo)

- **Unit of work:** public method on a resource class in
  `src/md_python/resources/` and `src/md_python/resources/v2/`.
- **Canonical path:** `src/md_python/resources/**/*.py`.
- **Extraction:** AST walk — for every class with a `client:` first-arg
  constructor, list every public method (no leading underscore). Capture
  `{class, method, signature, file, line, http_calls}`. `http_calls` greps for
  `self._client.get/post/put/patch/delete(...)` and records the URL fragment.
- **Cross-ref:** referenced by `mcp_tools.json` (`resource_calls`) and by
  `workflow.json` (`mcp_resource_method`). When a workflow endpoint exists but
  no resource method calls it, the endpoint is "client-unwrapped" → MCP cannot
  cover it without a new resource method.
- **Output:** `manifests/md_python_resources.json`.

---

## workflow (Rails — the API gateway)

- **Unit of work:** API endpoint (HTTP method + path + controller#action).
- **Canonical path:** `/Users/giuseppeinfusini/wd/md-repos/workflow`.
  - Authoritative source: `config/routes.rb` (483 lines).
  - The `api/` namespace under `app/controllers/api/` and `app/controllers/api/v2/`
    is the in-scope surface for MCP.
- **Extraction:** prefer `bin/rails routes --json` from inside the repo (gives
  controller + action + path + verb). Fallback when the Rails binstub is not
  runnable (no bundle, missing env): parse `config/routes.rb` with a regex
  scanner that recognises `resources :x`, `resource :x`, `get/post/put/patch/delete '...'`,
  `namespace :api` and `scope`. The fallback is lossy on dynamic route blocks —
  flag any block it cannot parse and ask the user.
- **Filter:** keep only routes whose path starts with `/api/`. Drop
  `devise_for`, admin UI routes (`/admins/*`), health probes,
  CSP-violation reporter, and routes under `deprecated/`.
- **Cross-ref:** `mcp_tool` field per route. Match by walking
  `md_python_resources.json` for resource methods that hit the same path
  fragment, then by walking `mcp_tools.json` for tools whose `resource_calls`
  point at that resource method.
- **Output:** `manifests/workflow.json`.

---

## data-set-service (job_slugs)

- **Unit of work:** `job_slug` — the identifier that drives `POST /datasets`
  (e.g. `pairwise-comparison`, `dose-response`, `normalisation-imputation`).
- **Canonical path:** `/Users/giuseppeinfusini/wd/md-repos/data-set-service`.
  - Slugs are stored in the database, not in code, and are validated at runtime
    by `src/services/job_run_params_validator.py` via
    `applicationContext.job_repository().get_by_slug(slug)`.
- **Extraction:** the canonical list is reachable at runtime via the service's
  own HTTP endpoint (`GET /jobs` per `src/routes/jobs.py`). Static extraction
  fallbacks, in order:
  1. `src/db/seeds/` or `db/seeds/` — seed file enumerating production jobs.
  2. `alembic/versions/*` — migrations that `INSERT INTO jobs` (`grep -RIn
     "INTO jobs" db/`).
  3. `tests/` — integration tests that reference slugs as string literals
     (`grep -RIn "job_slug" tests/`).
  4. last resort: spin up the service locally and call `GET /jobs`.
- **Schema awareness:** each job has a `params_schema`. The extractor records
  `{slug, params_schema, source_evidence}` where `source_evidence` cites the
  seed line or migration version that introduced it.
- **Cross-ref:** `mcp_tool` field per slug. Coverage is "covered" when an MCP
  pipeline tool (e.g. `run_dose_response`, `run_pairwise_comparison`) sends the
  slug.
- **Output:** `manifests/data_set_service.json`.

If the seed/migration extractors return nothing, ask the user to run a one-time
`GET /jobs` dump and check it in.

---

## visualisations-service (module_types)

- **Unit of work:** `module_type` (e.g. `pairwise_volcano_plot`).
- **Canonical extractor:** the existing `md-viz-modules` skill — its
  `extract.js` already parses both `workflow` (instruction + module classes)
  and `visualisations-service` (Python `Request` classes) into a per-module
  catalogue. **Do not duplicate this.**
- **What this skill does instead:** runs the md-viz-modules extractor in a
  dry-run mode that emits only the *list of module_type ids* plus the per-module
  parameter count and required-flag fingerprint. Compares the fingerprint
  against the manifest entry; a change means parameters were added/removed or
  defaults changed for an existing module.
- **Cross-ref:** the MCP exposes the catalogue via `list_module_types` and
  `describe_module_type`. Coverage is module-id-equality between the upstream
  catalogue and `list_module_types`'s output.
- **Output:** `manifests/visualisations_service.json`.

When a delta shows parameter changes, the dispatch task is *not* to wrap a new
MCP tool — `add_module_to_tab` already handles all module types generically.
The task is to **re-run md-viz-modules' extractor** so its catalogue stays
fresh, since that is what guides users in MCP calls.

---

## entity-mapping-service

- **Unit of work:** API endpoint (HTTP method + path).
- **Canonical path:** `/Users/giuseppeinfusini/wd/md-repos/entity-mapping-service`.
- **Extraction:** Python service — find every FastAPI / Flask route definition.
  Heuristics:
  - FastAPI: `@app.<verb>("/...")`, `@router.<verb>("/...")`
  - Grep: `grep -REn "@(app|router)\.(get|post|put|patch|delete)" src/`
- **Filter:** drop internal `/health`, `/metrics`, debug routes.
- **Cross-ref:** the MCP currently exposes `map_protein_to_protein`,
  `query_entities`, and the `entity_lists` family. Match by path fragment.
- **Output:** `manifests/entity_mapping_service.json`.

---

## md-converter

- **Unit of work:** supported input format (e.g. `maxquant`, `diann_tabular`,
  `tims_diann`, `spectronaut`, `md_format`, `md_format_gene`).
- **Canonical path:** `/Users/giuseppeinfusini/wd/md-repos/md-converter`.
- **Extraction:** the converter has one reader module per format. Heuristic:
  `find . -type d -name reader -o -name readers` for the package layout, then
  list the immediate subpackages.
- **Cross-ref:** the MCP enforces a closed enum in
  `src/md_python/resources/v2/uploads.py::ALLOWED_UPLOAD_SOURCES`. A new
  upstream format that isn't in `ALLOWED_UPLOAD_SOURCES` is "client-unwrapped";
  a format in `ALLOWED_UPLOAD_SOURCES` that doesn't exist upstream is a stale
  client entry.
- **Output:** `manifests/md_converter.json`.

---

## Adding a new source

1. Pick the unit of work — what discrete thing should the diff key on?
2. Add a `manifests/<source>.json` skeleton with `pinned_sha: null`, empty
   `coverage`, and a `recipe` block citing where the unit lives upstream.
3. Add a `extract_<source>` function to `scripts/coverage.py` that takes the
   repo path and returns a list of units.
4. Run `coverage.py bootstrap --source <name>`, review with the user, commit.
5. Document the recipe in this file.
