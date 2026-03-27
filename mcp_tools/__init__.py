from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "mass-dynamics",
    instructions="""
Mass Dynamics is a cloud proteomics data analysis platform. This MCP server
lets you upload proteomics data and run downstream analysis pipelines.

TOOL CATEGORIES — use roughly in this order:
  1. File tools    : read_csv_preview, load_metadata_from_csv, plan_wide_to_md_format
  2. Upload tools  : validate_upload_inputs, create_upload, wait_for_upload
  3. Dataset tools : find_initial_dataset, list_datasets, search_entities
  4. Pipeline tools: describe_pipeline, run_normalisation_imputation,
                     run_pairwise_comparison, run_anova, run_dose_response,
                     wait_for_dataset
  5. Utility       : health_check, list_jobs, batch, get_workflow_guide

CRITICAL RULES — always follow, no exceptions:
  - Never construct experiment_design or sample_metadata by hand.
    Always call load_metadata_from_csv on the user's CSV file first.
  - Always call describe_pipeline(<slug>) before running any pipeline.
  - Always call validate_upload_inputs before create_upload.
  - Never attempt to read proteomics data files (DIA-NN reports,
    MaxQuant proteinGroups.txt, Spectronaut exports, etc.) — they are
    uploaded as-is; the API processes them.
  - Before calling run_pairwise_comparison, run_normalisation_imputation,
    run_anova, or run_dose_response: present ALL parameters and their
    defaults to the user in a table and wait for explicit confirmation.
    Never choose statistical parameters autonomously.
  - All comparison pairs for a pairwise analysis go into ONE
    run_pairwise_comparison call. Never submit separate calls per pair.
  - INTENSITY is the correct dataset type for both raw upload datasets
    and NI pipeline output datasets. Do not treat it as unexpected.
  - RUNNING and PENDING are not failure states. Proteomics pipelines
    typically take 10–40 minutes. Call wait_for_dataset again without
    alarming the user. Only FAILED or ERROR require action.

Call get_workflow_guide() for step-by-step instructions and ready-to-paste
batch examples for common end-to-end workflows.
""",
)
