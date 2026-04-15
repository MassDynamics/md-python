import json

from . import mcp
from ._client import get_client


@mcp.tool()
def health_check() -> str:
    """Check the Mass Dynamics API health status."""
    result = get_client().health.check()
    return json.dumps(result, indent=2)


# ──────────────────────────────────────────────────────────────────────────────
# Workflow guide data — pure data structure, no logic.
# Extracted so tests can assert against the dict directly.
# ──────────────────────────────────────────────────────────────────────────────

_WORKFLOW_GUIDE = {
    "overview": (
        "Mass Dynamics is a cloud proteomics analysis platform. "
        "The general flow is: (1) prepare metadata CSV, (2) upload data files, "
        "(3) wait for ingestion, (4) run analysis pipelines, (5) view results in the app."
    ),
    "workflows": {
        "A_upload_new_data": {
            "description": "Upload a new proteomics experiment for the first time.",
            "steps": [
                "1. read_csv_preview(file_path) — inspect the metadata CSV to confirm it is a design/metadata file, not a proteomics data file.",
                "2. load_metadata_from_csv(file_path) — parse experiment_design and sample_metadata from the CSV. Never construct these arrays manually.",
                "   LFQ SHORTCUT: if the CSV has sample_name + condition but no filename column, suggest adding filename=sample_name (standard LFQ single-file setup).",
                "3. validate_upload_inputs(experiment_design, sample_metadata) — check for column mismatches before submitting.",
                "4. create_upload_from_csv(name, source, metadata_csv_path, file_location) — PREFERRED: loads metadata, validates, creates the upload, and backgrounds the file transfer. Returns immediately. Fall back to create_upload only if you already have experiment_design / sample_metadata arrays in memory AND the upload is S3-backed (local-file uploads through create_upload will time out).",
                "5. wait_for_upload(upload_id) — poll until COMPLETED (may take several minutes for large files).",
                "6. find_initial_dataset(upload_id) — get the INTENSITY dataset ID needed as input for all pipelines.",
            ],
            "notes": [
                "source must match the software that produced the data (see create_upload for valid values).",
                "If wait_for_upload returns FAILED/ERROR, the metadata or data files likely have format issues.",
            ],
        },
        "B_full_DEA": {
            "description": (
                "Full Differential Expression Analysis: upload → normalise/impute → pairwise comparison. "
                "Most common end-to-end workflow."
            ),
            "steps": [
                "── Phase 1: Upload (see Workflow A) ──",
                "1-6. Follow Workflow A steps to upload data and get the initial dataset ID.",
                "",
                "── Phase 2: Normalisation & Imputation ──",
                "7. describe_pipeline('normalisation_imputation') — inspect valid parameter values.",
                "8. run_normalisation_imputation(input_dataset_ids=[<initial_dataset_id>], dataset_name=..., normalisation_method='median', imputation_method='mnar') — start the pipeline.",
                "   For many uploads at once, use Workflow E (run_normalisation_imputation_bulk) instead.",
                "9. wait_for_dataset(upload_id, norm_dataset_id) — poll until COMPLETED.",
                "",
                "── Phase 3: Pairwise Comparison ──",
                "10. describe_pipeline('pairwise_comparison') — inspect valid parameter values.",
                "11. generate_pairwise_comparisons(sample_metadata, condition_column='condition', control=<control_name>) — build comparison pairs. Pass control= for case-vs-one-control designs (the common case); omit control= to generate all unique pairs. The returned list is the EXACT condition_comparisons to pass — do NOT filter it.",
                "12. run_pairwise_comparison(input_dataset_ids=[<norm_dataset_id>], dataset_name=..., sample_metadata=..., condition_column=..., condition_comparisons=...) — start limma analysis.",
                "13. wait_for_dataset(upload_id, pairwise_dataset_id) — poll until COMPLETED.",
                "    Results are now visible in the Mass Dynamics app.",
            ],
        },
        "C_full_DRA": {
            "description": (
                "Full Dose-Response Analysis: upload → normalise/impute → dose-response curves. "
                "Requires ≥3 distinct dose levels and ≥5 total replicates."
            ),
            "steps": [
                "── Phase 1 & 2: Upload + Normalise (same as DEA Phases 1-2) ──",
                "1-9. Follow Workflow B steps 1-9.",
                "",
                "── Phase 3: Dose-Response ──",
                "10. describe_pipeline('dose_response') — inspect valid parameter values.",
                "11. run_dose_response_from_upload(upload_id=..., dataset_name=..., sample_names=[...], control_samples=[...]) — PREFERRED: resolves input_dataset_id automatically; sample_metadata auto-fetched if omitted.",
                "    OR: run_dose_response(input_dataset_ids=[<norm_dataset_id>], dataset_name=..., sample_names=[...], control_samples=[...], sample_metadata=..., dose_column='dose') for explicit control.",
                "    sample_names and control_samples MUST come verbatim from sample_metadata rows — never infer them.",
                "    Ask the user which samples are controls (dose=0) if not obvious.",
                "12. wait_for_dataset(upload_id, dose_response_dataset_id) — poll until COMPLETED.",
            ],
            "notes": [
                "Minimum requirements: ≥3 distinct dose levels, ≥5 total replicates (3+ per dose recommended).",
                "control_samples are the samples at dose=0; they anchor the baseline of the 4PL curve.",
                "normalise defaults to 'none' — recommended when data was already normalised upstream.",
            ],
        },
        "E_bulk_multi_upload": {
            "description": (
                "Process many uploads at once — common for large studies (50–500 uploads). "
                "Uses bulk tools to submit and monitor all jobs in parallel."
            ),
            "steps": [
                "── Phase 1: Resolve dataset IDs ──",
                "1. find_initial_datasets(upload_ids=[...]) — bulk INTENSITY lookup, one call for all uploads.",
                "",
                "── Phase 2: Normalisation & Imputation ──",
                "2. run_normalisation_imputation_bulk(jobs=[{upload_id, dataset_name, normalisation_method, imputation_method}, ...]) — auto-resolves INTENSITY IDs, parallel submission, skips existing by default.",
                '3. wait_for_datasets_bulk(jobs=[{"dataset_id": <ni_id>}, ...]) — poll all NI jobs concurrently.',
                "   Call again until all_terminal is true. Pass failed items to retry_dataset.",
                "",
                "── Phase 3: Dose-Response or Pairwise ──",
                "4a. DR: run_dose_response_bulk(jobs=[{upload_id, dataset_name, sample_names, control_samples}, ...]) — sample_metadata auto-fetched per upload, cached across jobs for the same upload.",
                "4b. PC: run_pairwise_comparison_bulk(jobs=[{upload_id, input_dataset_ids, dataset_name, sample_metadata, condition_column, condition_comparisons}, ...]).",
                '5. wait_for_datasets_bulk(jobs=[{"dataset_id": <output_id>}, ...]) — poll all output jobs. Call again until all_terminal is true.',
            ],
            "notes": [
                "Prefer dataset_id-only job dicts in wait_for_datasets_bulk — upload_id is optional and only needed if you want list_by_upload routing.",
                "if_exists defaults to 'skip' in all bulk tools — safe to re-submit without creating duplicates.",
                "Max 500 jobs per bulk call. For >500, split into batches and call sequentially.",
                "Monitor progress by checking by_state in the wait_for_datasets_bulk response.",
            ],
        },
        "F_anova": {
            "description": (
                "Full ANOVA analysis: upload → normalise/impute → ANOVA. "
                "Use instead of pairwise comparison when the user wants an omnibus F-test "
                "across 3 or more conditions simultaneously."
            ),
            "steps": [
                "── Phase 1 & 2: Upload + Normalise (same as DEA Phases 1-2) ──",
                "1-9. Follow Workflow B steps 1-9.",
                "",
                "── Phase 3: ANOVA ──",
                "10. describe_pipeline('anova') — inspect valid parameter values.",
                "11. run_anova(input_dataset_ids=[<norm_dataset_id>], dataset_name=..., sample_metadata=..., condition_column=...) — start the ANOVA pipeline.",
                "    comparisons_type='all' (default): tests all pairwise combinations between condition levels.",
                "    comparisons_type='custom': also pass condition_comparisons=[[case, control], ...] to limit to specific pairs.",
                "12. wait_for_dataset(upload_id, anova_dataset_id) — poll until COMPLETED.",
                "    Results are now visible in the Mass Dynamics app.",
            ],
            "notes": [
                "ANOVA is an omnibus test — it detects any difference across groups but does not directly identify which pairs differ.",
                "For specific group-vs-group contrasts, use Workflow B (pairwise comparison) instead.",
                "sample_metadata must come from load_metadata_from_csv — never construct manually.",
            ],
        },
        "D_format_conversion": {
            "description": (
                "Convert a wide-format file (DIA-NN matrix, MaxQuant proteinGroups, Spectronaut export) "
                "to MD long format before uploading."
            ),
            "steps": [
                "1. plan_wide_to_md_format(file_path, source_hint='diann_tabular') — reads header only; returns a ready-to-run Python/pandas conversion script.",
                "2. Share the script with the user and ask them to run it locally (do NOT execute it yourself).",
                "3. Once the user has the converted file, follow Workflow A with source='md_format' (or 'md_format_gene'). CRITICAL: every row where ProteinIntensity / PeptideIntensity / GeneExpression = 0 MUST have Imputed=1 — a zero with Imputed=0 is treated as a real measurement and breaks pairwise/anova downstream.",
            ],
        },
        "G_dry_run_inspection": {
            "description": (
                "Inspect a local folder + metadata CSV before committing to "
                "create_upload_from_csv. Use when the user is unsure the files are ready."
            ),
            "steps": [
                "1. read_csv_preview(file_path=<metadata_csv>) — confirm it is a metadata CSV, not raw proteomics data.",
                "2. load_metadata_from_csv(file_path=<metadata_csv>) — parse the arrays. Do NOT call create_upload yet.",
                "3. validate_upload_inputs(experiment_design, sample_metadata) — check alignment.",
                "4. Show the user: source format, sample count, filename count from file_location, and any validation warnings. Wait for explicit go-ahead.",
                "5. Only then call create_upload_from_csv with the same inputs.",
            ],
            "notes": [
                "Pure inspection. No side effects. Use when the user wants a plan before committing to an upload.",
            ],
        },
        "H_retry_after_failure": {
            "description": (
                "A pipeline job returned FAILED, ERROR, or CANCELLED. Diagnose, then retry or delete."
            ),
            "steps": [
                "1. wait_for_dataset(upload_id, dataset_id) — confirm terminal state and capture the error message.",
                "2. list_datasets(upload_id) — confirm which inputs are still available.",
                "3. If the failure is transient (network / quota / worker crash): retry_dataset(dataset_id), then wait_for_dataset again.",
                "4. If parameters were wrong: delete_dataset(dataset_id) after user confirmation, then re-submit with corrected parameters.",
                "5. Do not retry more than twice in a row without checking in with the user.",
            ],
            "notes": [
                "retry_dataset reuses the same dataset_id. The retried run lands in place of the failed one.",
                "NOT_FOUND and FETCH_ERROR are terminal in wait_for_datasets_bulk — they indicate a bad id or an upstream fetch failure, not a pipeline failure. Do not retry them; escalate.",
            ],
        },
        "I_metadata_correction": {
            "description": (
                "Fix a typo or missing column in sample_metadata on an upload that already exists."
            ),
            "steps": [
                "1. get_upload_sample_metadata(upload_id) — fetch the current metadata as a 2D array.",
                "2. Show the array to the user and propose an exact diff. Wait for explicit 'yes, overwrite <upload_id>' confirmation — update_sample_metadata is DESTRUCTIVE.",
                "3. load_metadata_from_csv on the user's corrected CSV (preferred) OR apply the diff to the returned array.",
                "4. update_sample_metadata(upload_id, sample_metadata=<new_array>) — commits the overwrite.",
                "5. Any downstream datasets (NI, pairwise, anova, dose_response) submitted before the correction are now analytically stale. Ask the user whether to delete_dataset and re-run them.",
            ],
            "notes": [
                "update_sample_metadata replaces the whole array; there is no cell-level patch API.",
                "Sample names must still match exactly what the upload was created with — the backend links samples to files by name.",
            ],
        },
        "J_entity_lookup": {
            "description": (
                "Find specific proteins / genes / peptides in one or more datasets before "
                "or after running differential analysis."
            ),
            "steps": [
                "1. find_initial_dataset(upload_id) or list_datasets(upload_id) — collect the relevant dataset_ids.",
                "2. query_entities(keyword=<gene_symbol_or_uniprot>, dataset_ids=[...]) — server-side search; returns a {'results': [...]} JSON. Field names come from the server (gene_name, dataset_id, protein_accession) — parse defensively.",
                "3. Use the returned association to decide whether to run pairwise / anova, or to fetch a specific dataset table via download_dataset_table.",
            ],
            "notes": [
                "Keyword must be ≥2 characters. Matching is case-insensitive substring.",
                "Empty 'results' is a valid negative answer, not an error.",
            ],
        },
    },
    "tool_index": {
        "file_tools": {
            "read_csv_preview": "Inspect a CSV/TSV metadata file — columns and first N rows. Optional: max_rows (default 5), delimiter (auto-detected).",
            "load_metadata_from_csv": "Parse experiment_design and sample_metadata from a CSV file. Never construct these arrays manually. Optional: delimiter (auto-detected).",
            "get_md_format_spec": "Return the MD format column spec and a generic pandas conversion template for protein, peptide, or gene data. Call this when explaining the format to a user or writing custom conversion code without a file.",
            "plan_wide_to_md_format": "Generate a pandas conversion script for any wide-format intensity matrix → MD long format. Works for DIA-NN, MaxQuant, Spectronaut, or any generic CSV/TSV. Use annotation_columns to fix wrong auto-detection. Use transpose=True (or omit to auto-detect) when samples are rows and proteins are columns.",
        },
        "upload_tools": {
            "create_upload_from_csv": "SHORTCUT: load metadata CSV + validate + create upload in one call. Prefer over calling load_metadata_from_csv + validate_upload_inputs + create_upload separately.",
            "validate_upload_inputs": "Validate experiment_design and sample_metadata alignment before create_upload.",
            "create_upload": "Create a new upload. Prefer create_upload_from_csv for the common case.",
            "get_upload": "Fetch upload details by ID or name.",
            "get_upload_sample_metadata": "Fetch current sample_metadata for an upload as a 2D array. Round-trips into update_sample_metadata.",
            "update_sample_metadata": "Update sample metadata for an existing upload.",
            "wait_for_upload": "Poll an upload until COMPLETED/FAILED/ERROR/CANCELLED.",
            "list_uploads_status": "Check status of multiple uploads in one call. Pass summary=True to omit 'source' field — use this for large polls (100+ uploads) to reduce token overhead.",
            "query_uploads": "Paginated filter search over uploads (status/source/search/sample_metadata). 50/page. Prefer get_upload(name=...) for exact-name lookup.",
            "cancel_upload_queue": "Cancel queued (not yet started) background upload transfers.",
            "delete_upload": "Permanently delete an upload and its files. Fails with a friendly 409 message if the upload still has datasets — delete those first via delete_dataset.",
        },
        "dataset_tools": {
            "list_jobs": "Without upload_id: list global pipeline job catalog (slugs for describe_pipeline). With upload_id: list executed pipeline runs for that upload.",
            "list_datasets": "List all datasets for an upload. Optional: type_filter e.g. 'DOSE_RESPONSE' to restrict output.",
            "find_initial_dataset": "Find the INTENSITY dataset ID for one upload. For multiple uploads, prefer find_initial_datasets.",
            "find_initial_datasets": "BULK: find INTENSITY dataset IDs for many uploads in one call. Returns JSON {upload_id: {dataset_id}}.",
            "wait_for_dataset": "Poll one pipeline dataset until terminal state. Optional: poll_seconds (default 5), timeout_seconds (default 45).",
            "wait_for_datasets_bulk": "PREFERRED for many datasets: poll up to 500 datasets concurrently. Returns {total, all_terminal, by_state, pending, failed}. Call again when all_terminal is false.",
            "retry_dataset": "Retry a FAILED or ERROR pipeline job.",
            "delete_dataset": "Permanently delete a pipeline result dataset.",
            "cancel_dataset": "Cancel a RUNNING/PROCESSING pipeline job. Only valid for non-terminal datasets.",
            "query_datasets": "Paginated filter search over datasets (upload_id/state/type/search). 50/page. Prefer list_datasets(upload_id=...) for a single upload.",
            "download_dataset_table": "Get a presigned download URL for a dataset table (csv/parquet). Pass output_path to stream to disk instead.",
            "query_entities": "Search proteins, genes, or peptides by keyword (e.g. gene symbol or UniProt ID) across one or more datasets.",
        },
        "pipeline_tools": {
            "describe_pipeline": "Return the full parameter schema for a pipeline (valid_values, defaults). Call when you need to verify parameter values.",
            "run_normalisation_imputation": "Run normalisation + imputation for one upload. Usually the first pipeline after upload. Prefer run_normalisation_imputation_bulk for many uploads.",
            "run_normalisation_imputation_bulk": "PREFERRED for many NI jobs: auto-resolves INTENSITY dataset from upload_id, parallel submission (20 threads), max 500 jobs. if_exists='skip' by default.",
            "generate_pairwise_comparisons": "Generate [case, control] comparison pairs from sample_metadata.",
            "run_pairwise_comparison": "Run limma-based pairwise differential analysis for one upload. Prefer run_pairwise_comparison_bulk for many.",
            "run_pairwise_comparison_bulk": "PREFERRED for many pairwise jobs: parallel submission (20 threads), max 500 jobs, if_exists='skip' by default. input_dataset_ids must be explicit (use NI output IDs). Each job also needs upload_id (for dedup), sample_metadata, condition_column, condition_comparisons.",
            "run_anova": "Run ANOVA differential abundance across 3+ conditions using limma. Use instead of pairwise_comparison when the user wants an omnibus test across all groups. Supports comparisons_type='all' or 'custom'.",
            "run_dose_response": "Fit 4-parameter log-logistic dose-response curves. Prefer run_dose_response_from_upload (single) or run_dose_response_bulk (many).",
            "run_dose_response_from_upload": "PREFERRED for single DR job: resolves input_dataset_ids from upload_id automatically. if_exists='skip' by default; sample_metadata auto-fetched if omitted.",
            "run_dose_response_bulk": "PREFERRED for many DR jobs: parallel submission (20 threads), max 500 jobs, caches per-upload data, if_exists='skip' by default. Each job needs upload_id, dataset_name, sample_names, control_samples.",
        },
        "utility_tools": {
            "health_check": "Check API connectivity.",
            "get_workflow_guide": "This guide — call at the start of any session.",
            "batch": "Execute multiple tool calls in a single round-trip.",
        },
    },
    "constraints": [
        "NEVER construct experiment_design or sample_metadata manually. Always use load_metadata_from_csv.",
        "describe_pipeline(<slug>) is OPTIONAL — call it only when you need to verify a parameter value. It is NOT required before every pipeline run.",
        "ALWAYS call validate_upload_inputs before create_upload (create_upload_from_csv does this internally).",
        "Prefer create_upload_from_csv for local-file uploads. create_upload blocks on file transfer and will time out for large files.",
        "NEVER read proteomics data files (DIA-NN report.tsv, MaxQuant proteinGroups.txt, Spectronaut exports, md_format tables) — upload them as-is.",
        "sample_names and control_samples for dose-response must come verbatim from sample_metadata rows.",
        "Sample name matching is exact and case-sensitive across all tables.",
        "DESTRUCTIVE tools (delete_upload, delete_dataset, cancel_dataset, cancel_upload_queue, update_sample_metadata) require explicit user confirmation — echo the target id back before calling.",
        "Upload source must be one of: maxquant, diann_tabular, tims_diann, spectronaut, md_format, md_format_gene. Every other value is rejected client-side and server-side.",
    ],
    "common_mistakes": [
        "PAIRWISE — ONE CALL FOR ALL PAIRS: generate_pairwise_comparisons returns a list "
        "of N pairs. Pass that ENTIRE list as condition_comparisons to ONE single "
        "run_pairwise_comparison call. Do NOT loop, do NOT call once per pair, do NOT "
        "split by condition. limma must see all contrasts together for correct FDR "
        "correction. One dataset_id is returned and covers every pair in the list.",
        "WAITING: Do NOT treat RUNNING or PENDING as failure or stalled. Proteomics "
        "pipelines take 10–40 minutes. Call wait_for_dataset again without alarming the user.",
        "INTENSITY TYPE: NI pipeline output datasets are typed INTENSITY — same as the raw "
        "upload. This is correct. Do not flag it as unexpected or try to correct it.",
        "PARAMETERS: Never choose statistical parameters (normalisation/imputation method, "
        "filter logic, fit_separate_models, etc.) autonomously. Always present all parameters "
        "and defaults to the user in a table and wait for explicit confirmation.",
        "md_format IMPUTED FLAG: Every row where ProteinIntensity (or PeptideIntensity / "
        "GeneExpression) = 0.0 MUST have Imputed=1. A zero with Imputed=0 is treated as a "
        "real measurement and causes downstream pairwise jobs to fail. If source data uses "
        "0.0 for missing, run: long_df.loc[long_df['ProteinIntensity'] == 0, 'Imputed'] = 1",
        "METADATA: Never construct experiment_design or sample_metadata by hand. Always call "
        "load_metadata_from_csv on the user's CSV file.",
    ],
    "batch_tips": [
        "Use batch() to collapse independent or short sequential operations into one round-trip.",
        "Do NOT include wait_for_upload or wait_for_dataset inside a batch with other operations — they are long-running blocking calls; run them as standalone calls.",
        "Always use stop_on_error=True (default) for pipeline workflows.",
        "Prefer wait_for_datasets_bulk over repeated wait_for_dataset calls — even for a handful of jobs it is more efficient and returns a summary response.",
        "Example: batch([{'tool': 'load_metadata_from_csv', 'params': {'file_path': '...'}}, {'tool': 'validate_upload_inputs', 'params': {'experiment_design': [...], 'sample_metadata': [...]}}])",
    ],
}


@mcp.tool()
def get_workflow_guide() -> str:
    """Return step-by-step guidance for every common Mass Dynamics workflow.

    Call this at the start of any new session to orient yourself before using
    other tools. Returns a structured guide with workflow steps, tool index,
    batch usage patterns, and critical constraints.
    """
    return json.dumps(_WORKFLOW_GUIDE, indent=2)
