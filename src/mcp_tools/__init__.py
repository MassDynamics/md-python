from mcp.server.fastmcp import FastMCP

# ──────────────────────────────────────────────────────────────────────────────
# CONTRIBUTOR CONTRACT — error envelopes
#
# Every MCP tool returns ONE of:
#
#   * JSON-returning tools (their success return is a JSON string parsed by
#     the caller via ``json.loads``): on failure they MUST return
#     ``json.dumps({"error": "..."})``. Never return a bare ``"Error: ..."``
#     prose string from a JSON tool — it breaks ``json.loads`` for the caller.
#
#   * Prose-returning tools (their success return starts with a sentinel verb
#     like "Upload created.", "Dataset deleted successfully", "Module
#     placed."): on failure they MUST return a string starting with
#     ``"Error: "``. The legacy ``"Failed to <verb> <noun>"`` prefix is
#     accepted by the contract for back-compat (it appears in the body of
#     errors re-raised from SDK resources), but new code returns
#     ``f"Error: {e}"`` so the LLM can branch on a single sentinel.
#
# When you add a new tool, decide its return shape FIRST, then pick the
# envelope from this rule. The decision is mechanical: ``return json.dumps``
# of any payload → JSON envelope; ``return f"<verb sentence>"`` → prose
# envelope. Tests under tests/mcp_tools/test_error_envelopes.py check this.
# ──────────────────────────────────────────────────────────────────────────────

mcp = FastMCP(
    "mass-dynamics",
    instructions="""
Mass Dynamics is a cloud proteomics analysis platform. This MCP uploads
proteomics data and runs downstream statistical pipelines against it.

DATA vs WORKSPACE BOUNDARY — read before doing anything
Uploads, datasets, and pipeline runs are owned by the USER, not by a
workspace. They live at the account level and are discoverable from any
session via query_uploads / list_datasets / find_initial_dataset.
A workspace is purely a visual container of tabs and modules; it does
NOT own, contain, or store data. Workspace modules REFERENCE existing
uploads/datasets by id.

Practical consequences:
  * NEVER ask the user "which workspace should I upload into" — uploads
    have no workspace association.
  * NEVER create a workspace as a prerequisite for create_upload_from_csv,
    run_normalisation_imputation, run_pairwise_comparison, run_anova, or
    run_dose_response. Pipelines run against dataset_ids, not workspaces.
  * Only create or pick a workspace when the user explicitly wants to
    VIEW results (Workflow M_visualise) — at that point the workspace
    references existing dataset_ids the user already has.

RETURN-SHAPE CONTRACT
Every tool returns ONE of:
  - JSON string (parse with json.loads). Used by: health_check,
    get_workflow_guide, read_csv_preview, load_metadata_from_csv,
    get_md_format_spec, plan_wide_to_md_format, describe_pipeline,
    describe_entity_type,
    query_uploads, query_datasets, query_entities, map_protein_to_protein,
    map_gene_to_protein, map_protein_to_gene, map_protein_to_peptide,
    map_peptide_to_protein,
    get_upload_sample_metadata,
    list_uploads_status, find_initial_datasets, wait_for_datasets_bulk,
    download_dataset_table, run_normalisation_imputation_bulk,
    run_pairwise_comparison_bulk, run_dose_response_bulk,
    list_workspaces, get_workspace, list_tabs, get_tab,
    list_tab_modules, get_tab_module, list_entity_lists, get_entity_list,
    list_module_types, describe_module_type, render_module_visualisation,
    and list_jobs when called WITHOUT upload_id.
  - Prose string starting with a sentinel verb — "Upload created.",
    "Upload record created. ID:", "Normalisation/imputation pipeline
    started. Dataset ID:", "Pairwise comparison pipeline started. Dataset
    ID:", "ANOVA pipeline started. Dataset ID:", "Dose response pipeline
    started. Dataset ID:", "Sample metadata updated successfully",
    "Dataset deleted successfully", "Dataset retry triggered successfully",
    "Dataset cancellation requested", "Upload deleted successfully",
    "Workspace created. ID:", "Workspace deleted successfully. ID:",
    "Tab created. ID:", "Tab deleted successfully. ID:",
    "Module placed. ID:", "Text module placed. ID:",
    "Plotly JSON module placed. ID:", "Module removed successfully. ID:",
    "Entity list created. ID:", "Entity list deleted successfully. ID:".
    Branch on the sentinel, not on json.loads.
  - Error envelope: JSON tools return {"error": "..."}; prose tools return
    a string starting with "Error: ". Branch on this sentinel. The legacy
    prefix "Failed to <verb> <noun>" may still appear INSIDE the body of an
    Error string (re-raised SDK error message), but never as the leading
    sentinel of a tool return. Never treat a non-JSON string as JSON.

TOOL CATEGORIES — use roughly in this order:
  1. File tools    : read_csv_preview, load_metadata_from_csv,
                     get_md_format_spec, plan_wide_to_md_format
  2. Upload tools  : validate_upload_inputs, create_upload_from_csv
                     (PREFERRED for local files), create_upload,
                     wait_for_upload, get_upload, get_upload_sample_metadata,
                     update_sample_metadata, list_uploads_status,
                     query_uploads, cancel_upload_queue, delete_upload
  3. Dataset tools : find_initial_dataset, find_initial_datasets,
                     get_dataset, list_datasets, list_jobs,
                     wait_for_dataset,
                     wait_for_datasets_bulk, retry_dataset, delete_dataset,
                     cancel_dataset, query_datasets, download_dataset_table,
                     query_entities, map_protein_to_protein,
                     map_gene_to_protein, map_protein_to_gene,
                     map_protein_to_peptide, map_peptide_to_protein
  4. Pipeline tools: describe_pipeline, describe_entity_type,
                     run_normalisation_imputation,
                     run_normalisation_imputation_bulk,
                     generate_pairwise_comparisons, run_pairwise_comparison,
                     run_pairwise_comparison_bulk, run_anova,
                     run_dose_response, run_dose_response_from_upload,
                     run_dose_response_bulk, run_mofa
  5. Visualise tools: list_module_types, describe_module_type,
                     create_workspace, list_workspaces, get_workspace,
                     update_workspace, delete_workspace,
                     create_tab, list_tabs, get_tab, update_tab, delete_tab,
                     add_module_to_tab, list_tab_modules, get_tab_module,
                     update_tab_module, remove_module_from_tab,
                     add_text_module, update_text_module,
                     add_plotly_json_module, render_module_visualisation,
                     create_entity_list, list_entity_lists, get_entity_list,
                     update_entity_list, delete_entity_list
  6. Utility       : health_check, batch, get_workflow_guide

UPLOAD SOURCE FORMAT — enum (authoritative, enforced by the server and
by a client-side guard in src/md_python/resources/v2/uploads.py ::
ALLOWED_UPLOAD_SOURCES; mirrors workflow/app/models/experiment.rb:27-34):
  maxquant        MaxQuant: proteinGroups.txt + summary.txt
  diann_tabular   DIA-NN matrix: report.pg_matrix.tsv (+ optional
                  report.pr_matrix.tsv for peptide-level)
  tims_diann      DIA-NN long / timsTOF / PASER: report.tsv + DIA-NN log
  spectronaut     Spectronaut report.txt / .tsv / .csv
  md_format       MD long-format TSV (ProteinGroupId, ProteinGroup,
                  GeneNames, SampleName, ProteinIntensity, Imputed)
  md_format_gene  MD gene-level TSV (GeneId, GeneExpression, SampleName)
  md_format_metabolite  MD metabolite-level TSV (MetaboliteId,
                  MetaboliteIntensity, SampleName, Imputed)
Any other value (including raw, diann_raw, generic_format, simple,
unknown, diann_matrix, md_diann_maxlfq, msfragger) is rejected by the
client before a request is sent.

LABELLING: this client currently assumes LFQ. TMT uploads exist on the
server (maxquant supports TMT) but the MCP does not yet expose a
labelling_method parameter — contact support for TMT.

CRITICAL RULES — always follow:
  - Prefer create_upload_from_csv for any local-file upload. It reads,
    validates, creates the record, and backgrounds the transfer in one call.
  - Never construct experiment_design or sample_metadata by hand. Always
    call load_metadata_from_csv on the user's CSV first.
  - Never infer sample names from filenames or column headers — metadata
    must come verbatim from the user's metadata file.
  - Always call validate_upload_inputs before create_upload
    (create_upload_from_csv does this internally).
  - describe_pipeline(<slug>) is OPTIONAL — call it only when you need to
    verify a parameter value you are unsure of. It is NOT required before
    every pipeline run.
  - Before run_normalisation_imputation, run_pairwise_comparison, run_anova,
    or run_dose_response: present every parameter and its default to the
    user in a table and wait for explicit confirmation. Never choose
    statistical parameters autonomously, even when a default exists.
  - All comparison pairs for a pairwise analysis go into ONE
    run_pairwise_comparison call. limma must see all contrasts together
    for correct FDR correction — never loop one call per pair.
  - RUNNING and PENDING are not failures. Proteomics pipelines take 10-40
    minutes. Only FAILED, ERROR, or CANCELLED are terminal failure states.
  - INTENSITY is the correct dataset type for both raw upload datasets AND
    NI pipeline output datasets. Do not flag this as unexpected.
  - md_format (PROTEIN/PEPTIDE) rows where ProteinIntensity / PeptideIntensity
    equal 0 MUST have Imputed=1. A zero with Imputed=0 is treated as a real
    measurement and breaks pairwise / anova downstream. md_format_gene is
    DIFFERENT: md-converter auto-derives Imputed at md_format_gene/reader.py:
    120-124 (NaN or 0 → Imputed=1), so for gene uploads the Imputed column
    is OPTIONAL — required gene columns are only [GeneId, GeneExpression,
    SampleName] (md_format_gene/reader.py:8).

DESTRUCTIVE-ACTION RULE
Before calling any of these tools, echo the target id(s) back to the user,
describe what will change, and wait for an explicit "yes, <action> <id>":
  delete_upload, delete_dataset, cancel_dataset, cancel_upload_queue,
  update_sample_metadata, delete_workspace, delete_tab,
  remove_module_from_tab, delete_entity_list.
Never chain a destructive call after a query in the same turn.
(Source of truth: mcp_tools._destructive.DESTRUCTIVE_TOOL_NAMES — a
regression test fails if this list drifts.)

VISUALISATION RULE
Before calling add_module_to_tab or update_tab_module, the LLM MUST
present every parameter (from describe_module_type) to the user with
TWO defaults columns (platform default + LLM recommendation), declare
every data dependency the parameter requires (sample metadata, entity
lists, dataset type, …), and wait for explicit confirmation. Never auto-
pick. See each tool's docstring for the full mandate.

POLLING DISCIPLINE
Do not call wait_for_dataset or wait_for_upload more than ~10 times in a
row without checking in with the user. If the state is still RUNNING or
PENDING after ~10 polls, report progress ("still RUNNING after N checks,
~M minutes elapsed") and ask whether to keep waiting.

render_module_visualisation enforces a hard internal cap of ~10 HTTP
requests per MCP call. When the server is still rendering after that, the
tool returns ``{"status": "rendering", "polls": N, "retry_after": int,
"reason": "..."}``. Treat each subsequent call as ONE more "wait" toward
the same 10-poll check-in limit — after ~10 rendering envelopes in a row,
report progress to the user and ask before continuing.

ENTITY-DATA BOUNDARY — strict rule
Mass Dynamics handles all data and statistical processing. You must NEVER
read, inspect, parse, summarise, or process the user's proteomics data
files yourself (DIA-NN, MaxQuant, Spectronaut, md_format tables,
intensities, protein lists, raw / mass-spec files, etc.). Read ONLY
metadata CSVs (experiment_design, sample groupings). To discover entities
in a dataset, call query_entities — never by reading files.

Call get_workflow_guide() for step-by-step workflows.
""",
)
