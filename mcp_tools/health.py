import json

from . import mcp
from ._client import get_client


@mcp.tool()
def health_check() -> str:
    """Check the Mass Dynamics API health status."""
    result = get_client().health.check()
    return json.dumps(result, indent=2)


@mcp.tool()
def get_workflow_guide() -> str:
    """Return step-by-step guidance for every common Mass Dynamics workflow.

    Call this at the start of any new session to orient yourself before using
    other tools. Returns a structured guide with workflow steps, tool index,
    batch usage patterns, and critical constraints.
    """
    guide = {
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
                    "11. run_dose_response(input_dataset_ids=[<norm_dataset_id>], dataset_name=..., sample_names=[...], control_samples=[...], sample_metadata=..., dose_column='dose') — start curve fitting.",
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
                "read_csv_preview": "Inspect a CSV/TSV metadata file — columns and first N rows. Use before load_metadata_from_csv to confirm the file is a metadata file.",
                "load_metadata_from_csv": "Parse experiment_design and sample_metadata from a CSV file. Always call this first; never construct these arrays manually.",
                "plan_wide_to_md_format": "Generate a pandas conversion script for wide-format proteomics files (DIA-NN matrix, MaxQuant, Spectronaut) → MD long format.",
            },
            "upload_tools": {
                "validate_upload_inputs": "Validate experiment_design and sample_metadata alignment before create_upload.",
                "create_upload": "Create a new upload and trigger data ingestion.",
                "get_upload": "Fetch upload details by ID or name.",
                "update_sample_metadata": "Update sample metadata for an existing upload.",
                "wait_for_upload": "Poll an upload until COMPLETED/FAILED/ERROR/CANCELLED.",
            },
            "dataset_tools": {
                "list_jobs": "List available pipeline job types and their slugs.",
                "list_datasets": "List all datasets for an upload (inspection/debugging).",
                "find_initial_dataset": "Find the INTENSITY dataset ID needed as input for all run_* tools.",
                "wait_for_dataset": "Poll a pipeline dataset until COMPLETED/FAILED/ERROR/CANCELLED.",
                "retry_dataset": "Retry a FAILED or ERROR pipeline job.",
                "delete_dataset": "Permanently delete a pipeline result dataset.",
            },
            "pipeline_tools": {
                "describe_pipeline": "Return the full parameter schema for a pipeline. ALWAYS call before any run_* tool.",
                "run_normalisation_imputation": "Run normalisation + imputation. Usually the first pipeline after upload.",
                "generate_pairwise_comparisons": "Generate [case, control] comparison pairs from sample_metadata.",
                "run_pairwise_comparison": "Run limma-based differential abundance analysis.",
                "run_dose_response": "Fit 4-parameter log-logistic dose-response curves.",
            },
            "utility_tools": {
                "health_check": "Check API connectivity.",
                "get_workflow_guide": "This guide — call at the start of any session.",
                "batch": "Execute multiple tool calls in a single round-trip.",
            },
        },
        "constraints": [
            "NEVER construct experiment_design or sample_metadata manually. Always use load_metadata_from_csv.",
            "ALWAYS call describe_pipeline(<slug>) before any run_* pipeline tool.",
            "ALWAYS call validate_upload_inputs before create_upload.",
            "NEVER read proteomics data files (DIA-NN report.tsv, MaxQuant proteinGroups.txt, Spectronaut exports, MD_Format tables) — upload them as-is.",
            "sample_names and control_samples for dose-response must come verbatim from sample_metadata rows.",
            "Sample name matching is exact and case-sensitive across all tables.",
        ],
        "batch_tips": [
            "Use batch() to collapse independent or short sequential operations into one round-trip.",
            "Do NOT include wait_for_upload or wait_for_dataset inside a batch with other operations — they are long-running blocking calls; run them as standalone calls.",
            "Always use stop_on_error=True (default) for pipeline workflows.",
            "Example: batch([{'tool': 'load_metadata_from_csv', 'params': {'file_path': '...'}}, {'tool': 'validate_upload_inputs', 'params': {'experiment_design': [...], 'sample_metadata': [...]}}])",
        ],
    }
    return json.dumps(guide, indent=2)
