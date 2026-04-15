from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "mass-dynamics",
    instructions="""
Mass Dynamics is a cloud proteomics analysis platform. This MCP uploads
proteomics data and runs downstream statistical pipelines against it.

RETURN-SHAPE CONTRACT
Every tool returns ONE of:
  - JSON string (parse with json.loads). Used by: health_check,
    get_workflow_guide, read_csv_preview, load_metadata_from_csv,
    get_md_format_spec, plan_wide_to_md_format, describe_pipeline,
    query_uploads, query_datasets, query_entities, get_upload_sample_metadata,
    list_uploads_status, find_initial_datasets, wait_for_datasets_bulk,
    download_dataset_table, run_normalisation_imputation_bulk,
    run_pairwise_comparison_bulk, run_dose_response_bulk, and
    list_jobs when called WITHOUT upload_id.
  - Prose string starting with a sentinel verb — "Upload created.",
    "Upload record created. ID:", "Normalisation/imputation pipeline
    started. Dataset ID:", "Pairwise comparison pipeline started. Dataset
    ID:", "ANOVA pipeline started. Dataset ID:", "Dose response pipeline
    started. Dataset ID:", "Sample metadata updated successfully",
    "Dataset deleted successfully", "Dataset retry triggered successfully",
    "Dataset cancellation requested", "Upload deleted successfully".
    Branch on the sentinel, not on json.loads.
  - Error envelope: JSON tools return {"error": "..."}. Prose tools return
    strings starting with "Error:", "Failed to ...", or "STOP". Never
    treat a non-JSON string as JSON.

TOOL CATEGORIES — use roughly in this order:
  1. File tools    : read_csv_preview, load_metadata_from_csv,
                     get_md_format_spec, plan_wide_to_md_format
  2. Upload tools  : validate_upload_inputs, create_upload_from_csv
                     (PREFERRED for local files), create_upload,
                     wait_for_upload, get_upload, get_upload_sample_metadata,
                     update_sample_metadata, list_uploads_status,
                     query_uploads, cancel_upload_queue, delete_upload
  3. Dataset tools : find_initial_dataset, find_initial_datasets,
                     list_datasets, list_jobs, wait_for_dataset,
                     wait_for_datasets_bulk, retry_dataset, delete_dataset,
                     cancel_dataset, query_datasets, download_dataset_table,
                     query_entities
  4. Pipeline tools: describe_pipeline, run_normalisation_imputation,
                     run_normalisation_imputation_bulk,
                     generate_pairwise_comparisons, run_pairwise_comparison,
                     run_pairwise_comparison_bulk, run_anova,
                     run_dose_response, run_dose_response_from_upload,
                     run_dose_response_bulk
  5. Utility       : health_check, batch, get_workflow_guide

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
  - md_format rows where ProteinIntensity / PeptideIntensity / GeneExpression
    equal 0 MUST have Imputed=1. A zero with Imputed=0 is treated as a real
    measurement and breaks pairwise / anova downstream.

DESTRUCTIVE-ACTION RULE
Before calling any of these tools, echo the target id(s) back to the user,
describe what will change, and wait for an explicit "yes, <action> <id>":
  delete_upload, delete_dataset, cancel_dataset, cancel_upload_queue,
  update_sample_metadata.
Never chain a destructive call after a query in the same turn.

POLLING DISCIPLINE
Do not call wait_for_dataset or wait_for_upload more than ~10 times in a
row without checking in with the user. If the state is still RUNNING or
PENDING after ~10 polls, report progress ("still RUNNING after N checks,
~M minutes elapsed") and ask whether to keep waiting.

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
