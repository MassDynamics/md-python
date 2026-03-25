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
                "4. create_upload(name, source, experiment_design, sample_metadata, file_location=...) — submit the upload.",
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
                "8. run_normalisation_imputation(input_dataset_ids=[<initial_dataset_id>], dataset_name=..., normalisation_method='median', imputation_method='min_value') — start the pipeline.",
                "   For many uploads at once, use Workflow E (run_normalisation_imputation_bulk) instead.",
                "9. wait_for_dataset(upload_id, norm_dataset_id) — poll until COMPLETED.",
                "",
                "── Phase 3: Pairwise Comparison ──",
                "10. describe_pipeline('pairwise_comparison') — inspect valid parameter values.",
                "11. generate_pairwise_comparisons(sample_metadata, condition_column='condition') — build comparison pairs.",
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
        "D_format_conversion": {
            "description": (
                "Convert a wide-format file (DIA-NN matrix, MaxQuant proteinGroups, Spectronaut export) "
                "to MD long format before uploading."
            ),
            "steps": [
                "1. plan_wide_to_md_format(file_path, source_hint='diann_matrix') — reads header only; returns a ready-to-run Python/pandas conversion script.",
                "2. Share the script with the user and ask them to run it locally (do NOT execute it yourself).",
                "3. Once the user has the converted file, follow Workflow A with source='md_format' (or 'md_format_gene').",
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
            "update_sample_metadata": "Update sample metadata for an existing upload.",
            "wait_for_upload": "Poll an upload until COMPLETED/FAILED/ERROR/CANCELLED.",
            "list_uploads_status": "Check status of multiple uploads in one call. Pass summary=True to omit 'source' field — use this for large polls (100+ uploads) to reduce token overhead.",
            "cancel_upload_queue": "Cancel queued (not yet started) background upload transfers.",
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
        },
        "pipeline_tools": {
            "describe_pipeline": "Return the full parameter schema for a pipeline (valid_values, defaults). Call when you need to verify parameter values.",
            "run_normalisation_imputation": "Run normalisation + imputation for one upload. Usually the first pipeline after upload. Prefer run_normalisation_imputation_bulk for many uploads.",
            "run_normalisation_imputation_bulk": "PREFERRED for many NI jobs: auto-resolves INTENSITY dataset from upload_id, parallel submission (20 threads), max 500 jobs. if_exists='skip' by default.",
            "generate_pairwise_comparisons": "Generate [case, control] comparison pairs from sample_metadata.",
            "run_pairwise_comparison": "Run limma-based pairwise differential analysis for one upload. Prefer run_pairwise_comparison_bulk for many.",
            "run_pairwise_comparison_bulk": "PREFERRED for many pairwise jobs: parallel submission (20 threads), max 500 jobs, if_exists='skip' by default. input_dataset_ids must be explicit (use NI output IDs). Each job also needs upload_id (for dedup), sample_metadata, condition_column, condition_comparisons.",
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
        "Call describe_pipeline(<slug>) if you need to verify valid parameter values before running a pipeline — optional when parameters are already known.",
        "ALWAYS call validate_upload_inputs before create_upload.",
        "NEVER read proteomics data files (DIA-NN report.tsv, MaxQuant proteinGroups.txt, Spectronaut exports, MD_Format tables) — upload them as-is.",
        "sample_names and control_samples for dose-response must come verbatim from sample_metadata rows.",
        "Sample name matching is exact and case-sensitive across all tables.",
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
