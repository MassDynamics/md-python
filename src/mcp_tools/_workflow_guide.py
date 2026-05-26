"""Static prose data backing the ``get_workflow_guide`` MCP tool.

Extracted from ``mcp_tools.health`` so the tool function stays small. Tests
import the dict via ``mcp_tools.health._WORKFLOW_GUIDE`` — that path stays
live because ``health.py`` re-exports the symbol.
"""

_WORKFLOW_GUIDE = {
    "overview": (
        "Mass Dynamics is a cloud proteomics analysis platform. "
        "The general flow is: (1) prepare metadata CSV, (2) upload data files, "
        "(3) wait for ingestion, (4) run analysis pipelines, (5) view results in the app. "
        "DATA vs WORKSPACE BOUNDARY: uploads, datasets, and pipeline runs are owned by "
        "the user at the account level — they have NO workspace association and are "
        "discoverable from any session. A workspace is purely a visual container of tabs "
        "and modules; it does NOT own or store data. Workspace modules REFERENCE existing "
        "datasets by id. Do NOT ask 'which workspace should I upload into', do NOT create "
        "a workspace as a prerequisite for any upload or pipeline tool, and only involve "
        "a workspace when the user explicitly wants to VIEW results (Workflow M)."
    ),
    "analysis_mandates": [
        "MANDATORY Q&A: Before calling any analysis pipeline tool the LLM MUST present "
        "every parameter in a table, explain each in plain language, and wait for "
        "explicit user confirmation. Bulk variants confirm ONCE for the whole batch. "
        "Never auto-pick — even when a default exists.",
        "TWO-DEFAULTS MANDATE: The parameter table MUST contain TWO defaults columns: "
        "(a) PLATFORM DEFAULT — the canonical Mass Dynamics default with a source-of-"
        "truth citation, and (b) LLM RECOMMENDATION — what the LLM thinks the user "
        "should use given the experiment context, justified in one sentence. Mark "
        "rows '(diverges)' when the recommendation diverges from the platform default.",
        "Pre-fill values ONLY when explicitly labelled 'LLM recommendation, please "
        "confirm or change.' Silence is NOT confirmation.",
    ],
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
                "3. Once the user has the converted file, follow Workflow A with source='md_format' (protein/peptide), 'md_format_gene' (gene), or 'md_format_metabolite' (metabolite). PROTEIN/PEPTIDE: every row where ProteinIntensity / PeptideIntensity = 0 MUST have Imputed=1 — a zero with Imputed=0 is treated as a real measurement and breaks pairwise/anova downstream. GENE: do NOT pre-write Imputed; md-converter auto-derives it (NaN or 0 → Imputed=1) from md_format_gene/reader.py:120-124. Required gene columns are only [GeneId, GeneExpression, SampleName] (reader.py:8). METABOLITE: columns are [MetaboliteId, MetaboliteIntensity, SampleName, Imputed]; Imputed IS required and validated 0/1 (md_format_metabolite/reader.py:8,80-82) — set 1 where MetaboliteIntensity=0.",
                "ALL md_format* outputs are LONG format and MUST be a FULL matrix: exactly one row per entity per sample, every entity x sample combination present, NO EXCEPTIONS. A non-measurement is a 0.0 row with Imputed=1, never an absent row. The plan_wide_to_md_format script produces this by construction.",
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
        "K_filtration_only": {
            "description": (
                "Run filtration WITHOUT changing values — produces a filtered INTENSITY "
                "dataset that downstream pipelines can consume directly. Typical when the "
                "user wants to drop entities by missing-values completeness "
                "(protein/peptide), low PTM-localization probability (peptide), or low "
                "abundance after CPM (gene)."
            ),
            "steps": [
                "1. find_initial_dataset(upload_id) — get the upload-created INTENSITY id "
                "(disambiguates when an upload already has multiple INTENSITY datasets).",
                "2. get_upload_sample_metadata(upload_id) — needed for experiment_design.",
                "3. run_normalisation_imputation("
                "input_dataset_ids=[<initial_id>], dataset_name=..., "
                "normalisation_method='skip', imputation_method='skip', "
                "filtration_method='by missing values' | 'by ptm localization probability' | "
                "'by minimum abundance', "
                "filtration_extra_params={...}) — start the filter-only run. The output is "
                "still an INTENSITY dataset.",
                "4. wait_for_dataset(upload_id, output_id) — poll until COMPLETED.",
            ],
            "notes": [
                "Output type is INTENSITY, identical to the upload-created dataset. find_initial_dataset disambiguates by picking the unique INTENSITY with no upstream input.",
                "Per entity_type: protein/peptide → 'by missing values'; peptide also → 'by ptm localization probability'; gene → 'by minimum abundance'.",
                "Pass experiment_design = SampleMetadata(...).to_columns() inside filtration_extra_params (or via the typed builder kwarg).",
            ],
        },
        "L_gene_workflow": {
            "description": (
                "End-to-end gene workflow: md_format_gene upload → NI (with cpm + "
                "by minimum abundance) → pairwise / ANOVA. Gene differential analysis "
                "supports de_method ∈ {limma, edgeR, DESeq2} via the entity-keyed "
                "de_method_gene wire field; protein / peptide / metabolite / ptm are "
                "limma-only."
            ),
            "steps": [
                "── Phase 1: Upload ──",
                "1. read_csv_preview / load_metadata_from_csv on the metadata.",
                "2. create_upload_from_csv(name=..., source='md_format_gene', metadata_csv_path=..., file_location=...).",
                "3. wait_for_upload(upload_id).",
                "4. find_initial_dataset(upload_id) — INTENSITY id (gene level).",
                "",
                "── Phase 2: NI (gene-aware) ──",
                "5. describe_pipeline('normalisation_imputation') — confirm gene-allowed "
                "values (cpm, batch correction with combat seq, by minimum abundance).",
                "6. run_normalisation_imputation(input_dataset_ids=[<initial_id>], "
                "dataset_name=..., entity_type='gene', normalisation_method='cpm', "
                "imputation_method='skip', filtration_method='by minimum abundance', "
                "filtration_extra_params={'minimum_abundance_threshold': ..., "
                "'filter_valid_values_criteria': 'percentage', "
                "'filter_threshold_proportion': 0.5, "
                "'filter_valid_values_logic': 'at least one condition', "
                "'filter_based_on_condition': 'condition', "
                "'experiment_design': <SampleMetadata.to_columns()>}).",
                "7. wait_for_dataset(upload_id, ni_id).",
                "",
                "── Phase 3: Pairwise OR ANOVA (limma | edgeR | DESeq2) ──",
                "8a. run_pairwise_comparison(input_dataset_ids=[<ni_id>], dataset_name=..., "
                "sample_metadata=..., condition_column=..., condition_comparisons=..., "
                "entity_type='gene', de_method='limma' | 'edgeR' | 'DESeq2'). edgeR/"
                "DESeq2 carry companion params — see describe_pipeline('pairwise_"
                "comparison') for the full menu.",
                "8b. run_anova(input_dataset_ids=[<ni_id>], dataset_name=..., "
                "sample_metadata=..., condition_column=..., entity_type='gene', "
                "de_method='limma' | 'edgeR' | 'DESeq2').",
                "9. wait_for_dataset(upload_id, output_id).",
            ],
            "notes": [
                "source='md_format_gene' is the gene-specific MD long-format upload. Required file columns: GeneId, GeneExpression, SampleName (md-converter md_format_gene/reader.py:8 REQUIRED_GENE_COLUMNS). Imputed is OPTIONAL — md-converter auto-derives it (NaN or 0 → Imputed=1) at md_format_gene/reader.py:120-124.",
                "experiment_design is OPTIONAL for md_format_gene uploads — workflow skips the 'experiment_design required' validation for source=md_format_gene (workflow/app/models/experiment.rb:98-103). sample_metadata IS still required.",
                "If the user has a metadata CSV with both filename and sample_name (the standard LFQ shape), pass it through load_metadata_from_csv as usual — the resulting experiment_design is harmlessly accepted.",
                "cpm normalisation is gene-only. ComBat-Seq is gene-only batch correction; ComBat and Limma remove batch effect work for gene too.",
                "Gene pairwise / ANOVA now accept de_method ∈ {limma, edgeR, DESeq2}. The wire field is entity-keyed (de_method_gene). Protein / peptide / metabolite / ptm are limma-only — the MCP rejects edgeR/DESeq2 for them client-side before submission.",
            ],
        },
        "M_visualise": {
            "description": (
                "Place dashboard modules on a workspace tab — the visual "
                "layer of the app. Workspace → Tab → Module on a "
                "react-grid-layout grid. A workspace is purely a visual "
                "container; it does NOT own or contain data. Modules "
                "REFERENCE existing uploads/datasets by id — the user's "
                "uploads exist independently and are visible from any "
                "workspace (and from no workspace at all). Only enter this "
                "workflow when the user explicitly asks to VIEW results; "
                "never create a workspace as a prerequisite for uploading "
                "data or running a pipeline. Every module (volcano, "
                "heatmap, PCA, dose-response curves, …) declares its "
                "parameters via the registry; the LLM MUST walk the user "
                "through every one before placing the module. Server does "
                "NOT merge registry defaults at create time, so "
                "add_module_to_tab always sends the full settings hash via "
                "the client's create_with_defaults helper."
            ),
            "steps": [
                "── Phase 1: Discover ──",
                "1. list_module_types() — index of every module type "
                "available to the current user (filtered by feature flags). "
                "Pick item_id by group (Pairwise analysis, Heatmap, "
                "Experiment, ANOVA, Dose-response, Quality control, etc.).",
                "2. describe_module_type(item_id) — full parameter docs. "
                "Read every parameter, every default, every "
                "data_dependency. Quote the long-form `description` to the "
                "user when explaining what the module computes.",
                "",
                "── Phase 2: Resolve data dependencies ──",
                "3. For Datasets parameters: find_initial_dataset / "
                "list_datasets / query_datasets — get the dataset_id of the "
                "type required by parameters.type (INTENSITY for QC + "
                "experiment plots, PAIRWISE for volcano/heatmap, "
                "DOSE_RESPONSE for DR curves, ANOVA for ANOVA volcano).",
                "4. For DatasetSampleMetadata / DatasetSampleMetadataValues "
                "/ OrderableSampleMetadataColumns / "
                "SampleMetadataValuesFilter parameters: "
                "get_upload_sample_metadata(upload_id) — fetch the 2D "
                "metadata array; the column names live in row 0, the "
                "values in rows 1+.",
                "5. For ProteinList / ProteinLists / ProteinSelection: "
                "call list_entity_lists(workspace_id) to discover existing "
                "lists (paginated, 50/page), or query_entities(keyword=...) "
                "to assemble explicit protein-group ids. Use "
                "create_entity_list to persist a new selection.",
                "6. For ConditionComparison: list_datasets to find the "
                "PAIRWISE dataset; the comparisons are in "
                "Dataset.job_run_params.condition_comparisons.",
                "",
                "── Phase 3: Parameter Q&A ──",
                "7. Build a parameter table with TWO defaults columns "
                "(platform default + LLM recommendation) and present it to "
                "the user. Never elide rows — even null defaults, even "
                "optional fields, even unmapped fieldTypes. Mark "
                "(diverges) on rows where the LLM recommendation differs "
                "from the platform default. Wait for explicit confirmation.",
                "",
                "── Phase 4: Place + iterate ──",
                "8. create_workspace(name=...) if no workspace exists yet. "
                "An API-created workspace has ZERO tabs initially; the app "
                "lazily creates a default 'new tab' the first time the "
                "user opens the workspace in the UI (frontend code at "
                "app/javascript/workspaces/repositories/"
                "WorkspaceTabsRepository.js#L8-28).",
                "9. list_tabs(workspace_id) — REUSE-FIRST. If an existing "
                "tab is there (typically named 'new tab' from the UI's "
                "auto-creation), reuse its tab_id for add_module_to_tab. "
                "Only call create_tab when the user explicitly asks for "
                "an additional tab, NOT to set up the first one.",
                "10. add_module_to_tab(workspace_id, tab_id, item_id, x, y, "
                "width, height, settings={confirmed values}) — the wrapper "
                "fills every registry default the LLM did not override.",
                "11. update_tab_module(...) to move/resize/re-configure. "
                "Always re-send item_id and the full settings hash on PUT "
                "(see tool docstring for the two server-side contracts).",
                "12. remove_module_from_tab / delete_tab / delete_workspace "
                "for cleanup — destructive, ask for explicit confirmation.",
            ],
            "notes": [
                "The canvas is 12 grid columns wide. Headings / text / "
                "page_break are typically 12x1. PLOT modules (volcano, "
                "heatmap, PCA, dose-response, box plot, every Quality "
                "control plot, etc.) MUST be sized with height >= 12 — "
                "smaller heights crop the visualisation and collapse "
                "legends on top of the data. Width is 6 (half-canvas, "
                "two side-by-side) or 12 (full-width).",
                "Any settings key not in the module's input_settings → 400 "
                "from the server. Stick to the keys describe_module_type "
                "returns.",
                "Server bug: PUT module endpoint requires item_id even on "
                "partial updates (existing['item_id'] vs persistence "
                "'itemId' mismatch). update_tab_module re-sends it for you, "
                "but you must pass it.",
                "Server bug: PUT module endpoint replaces settings "
                "wholesale — rebuild the full hash from "
                "describe_module_type.registry_defaults + the existing "
                "settings + your change.",
                "Server bug: registry-declared defaults are NOT applied "
                "server-side at create. add_module_to_tab works around "
                "this by sending every default; if you ever bypass it and "
                "POST raw partial settings, the rendered widget will "
                "surface 'Please provide ...' prompts.",
                "Reuse-first tab rule: the app UI lazily creates a tab "
                "named 'new tab' when the user opens an empty workspace "
                "(WorkspaceTabsRepository.js#L8-28). Always call list_tabs "
                "before create_tab — if a tab already exists, reuse it; "
                "do NOT add a parallel default tab.",
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
                "3. (Optional, protein-only) map_protein_to_protein(dataset_ids=[...], entity_ids=[<protein_group_ids>]) — returns {'nodes': [...], 'edges': [...]} linking the queried protein groups to other groups through shared individual proteins. Use when the user asks about isoform families, shared-peptide ambiguity, or cross-experiment protein-group overlap.",
                "4. Use the returned association(s) to decide whether to run pairwise / anova, or to fetch a specific dataset table via download_dataset_table.",
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
            "get_md_format_spec": "Return the MD format column spec and a generic pandas conversion template for protein, peptide, gene, or metabolite data. Call this when explaining the format to a user or writing custom conversion code without a file.",
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
            "find_initial_dataset": "Find the upload-created INTENSITY dataset ID. After NI/filter runs there are 2+ INTENSITY datasets per upload; this disambiguates by picking the unique one with no upstream input. For multiple uploads, prefer find_initial_datasets.",
            "find_initial_datasets": "BULK: find INTENSITY dataset IDs for many uploads in one call. Returns JSON {upload_id: {dataset_id}}.",
            "wait_for_dataset": "Poll one pipeline dataset until terminal state. Optional: poll_seconds (default 5), timeout_seconds (default 45).",
            "wait_for_datasets_bulk": "PREFERRED for many datasets: poll up to 500 datasets concurrently. Returns {total, all_terminal, by_state, pending, failed}. Call again when all_terminal is false.",
            "retry_dataset": "Retry a FAILED or ERROR pipeline job.",
            "delete_dataset": "Permanently delete a pipeline result dataset.",
            "cancel_dataset": "Cancel a RUNNING/PROCESSING pipeline job. Only valid for non-terminal datasets.",
            "query_datasets": "Paginated filter search over datasets (upload_id/state/type/search). 50/page. Prefer list_datasets(upload_id=...) for a single upload.",
            "download_dataset_table": "Get a presigned download URL for a dataset table (csv/parquet). Pass output_path to stream to disk instead.",
            "query_entities": "Search proteins, genes, or peptides by keyword (e.g. gene symbol or UniProt ID) across one or more datasets.",
            "map_protein_to_protein": "Graph nodes + edges connecting protein groups through their shared individual proteins, scoped to one or more datasets. Use after query_entities to inspect shared-peptide ambiguity, isoform families, or cross-dataset protein-group provenance. Returns {nodes, edges} JSON.",
            "map_gene_to_protein": "Graph nodes + edges linking gene entities to protein groups via the protein cross-reference, across one or more datasets. Use to translate a gene-symbol hit into the actual protein groups quantified in the user's data. Returns {nodes, edges} JSON.",
            "map_protein_to_gene": "Graph nodes + edges linking protein groups to gene entities via the protein cross-reference, across one or more datasets. Use to roll a set of protein-group hits up to their underlying genes for enrichment, pathway lookup, or annotation cross-reference. Returns {nodes, edges} JSON.",
            "map_protein_to_peptide": "Graph nodes + edges linking protein groups to their peptides within ONE dataset. Use when the user wants the peptide composition of specific protein groups (peptide-level QC, evidence inspection). Returns {nodes, edges} JSON.",
            "map_peptide_to_protein": "Graph nodes + edges linking peptides to their protein groups within ONE dataset. Use when peptide-level hits need rolling up to protein groups. Returns {nodes, edges} JSON.",
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
        "visualise_tools": {
            "list_module_types": "List every dashboard module type available to the current user — id, name, group, short description, registry-default coverage. Discovery layer for the visualise flow.",
            "describe_module_type": "Full structured parameter docs for one module type. Returns every parameter (even null defaults, even optional fields) with field type, value semantics, data dependencies, conditional-visibility clauses, cross-field refs, options, and the prose description. The source-of-truth for the parameter Q&A mandate.",
            "create_workspace": "Create a workspace — the top-level container for tabs.",
            "list_workspaces": "List workspaces accessible to the current user (paginated, 50/page).",
            "get_workspace": "Fetch a single workspace by id.",
            "update_workspace": "Update workspace name and/or description (partial — only fields you pass are sent).",
            "delete_workspace": "DESTRUCTIVE — deletes the workspace plus every tab and module inside it.",
            "create_tab": "Create a tab in a workspace. tab_index is auto-assigned to max+1 server-side.",
            "list_tabs": "List tabs in a workspace, ordered by tab_index ascending.",
            "get_tab": "Fetch a single tab by id.",
            "update_tab": "Partial update of name/layout/settings. Locked tabs reject updates.",
            "delete_tab": "DESTRUCTIVE — deletes a tab and every module inside its layout. Locked tabs cannot be deleted.",
            "add_module_to_tab": "Place a module on a tab's grid. Uses create_with_defaults under the hood — every registry default is sent on the wire even when not changed. MANDATORY parameter Q&A first (see visualisation mandate).",
            "list_tab_modules": "List every module currently on a tab (no pagination).",
            "get_tab_module": "Fetch a single module by id.",
            "update_tab_module": "Move/resize/re-configure a module. item_id is REQUIRED on every PUT (server-side bug). settings is REPLACED wholesale (no per-key merge) — always rebuild the full hash from registry_defaults + existing + your change.",
            "remove_module_from_tab": "DESTRUCTIVE — remove a module from a tab.",
            "add_text_module": "Add a text/narrative module (HTML body in settings.text). Bypasses the visualisation Q&A mandate — there is only one user-supplied value (the body). Default size 12x3.",
            "update_text_module": "Replace the body of an existing text module. Sends only settings.text so layout (x/y/width/height) is preserved server-side. Use update_tab_module if you also need to move/resize.",
            "create_entity_list": "Create a named entity list (proteins/peptides/genes) scoped to a workspace. Items are (entity_id, group_id, dataset_id) triples. Save the returned id and reuse it; you can also rediscover the list later via list_entity_lists.",
            "list_entity_lists": "Paginated list (50/page) of entity lists in a workspace. Each entry has the same shape as get_entity_list, including its items array. Use this to rediscover entity_list ids the LLM did not save during creation.",
            "get_entity_list": "Fetch a single entity list by id, scoped to its workspace. Returns the list + items.",
            "update_entity_list": "Replace an entity list's name, entity_type, and items wholesale (server does a full replace). Call get_entity_list first to read the current state if you only want to change one field.",
            "delete_entity_list": "DESTRUCTIVE — permanently delete an entity list. Modules referencing its proteinListId / entityListId will surface 'Please provide ...' prompts afterwards. Echo the list_id back and require explicit 'yes, delete <id>' confirmation.",
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
        "DESTRUCTIVE tools (delete_upload, delete_dataset, cancel_dataset, cancel_upload_queue, update_sample_metadata) carry the MANDATORY DESTRUCTIVE-ACTION CONFIRMATION mandate (see each tool's docstring): (1) echo every target id back to the user, (2) state the consequence in plain language, (3) wait for explicit 'yes, <verb> <id>' confirmation — silence and bare 'OK' are NOT enough, (4) never chain a destructive call after a query in the same turn.",
        "Upload source must be one of: maxquant, diann_tabular, tims_diann, spectronaut, md_format, md_format_gene, md_format_metabolite. Every other value is rejected client-side and server-side.",
    ],
    "common_mistakes": [
        "PAIRWISE — ONE CALL FOR ALL PAIRS: generate_pairwise_comparisons returns a list "
        "of N pairs. Pass that ENTIRE list as condition_comparisons to ONE single "
        "run_pairwise_comparison call. Do NOT loop, do NOT call once per pair, do NOT "
        "split by condition. limma must see all contrasts together for correct FDR "
        "correction. One dataset_id is returned and covers every pair in the list.",
        "WAITING: Do NOT treat RUNNING or PENDING as failure or stalled. Proteomics "
        "pipelines take 10–40 minutes. Call wait_for_dataset again without alarming the user.",
        "INTENSITY TYPE: NI / filter-only pipeline output datasets are typed INTENSITY — "
        "same as the raw upload. This is correct. After running NI or filter-only, an "
        "upload has 2+ INTENSITY datasets; find_initial_dataset disambiguates by selecting "
        "the unique INTENSITY with no upstream input.",
        "PARAMETERS — MANDATORY Q&A: Before submitting any analysis pipeline tool "
        "(run_normalisation_imputation, run_pairwise_comparison, run_anova, "
        "run_dose_response and their bulk variants), the LLM MUST present every "
        "parameter (required AND tuneable) in a table to the user, explain what each "
        "does in plain language, and wait for explicit confirmation. Bulk variants ask "
        "ONCE for the whole batch. Never auto-pick — even when a default exists.",
        "PARAMETERS — TWO-DEFAULTS MANDATE: The parameter table must show TWO "
        "defaults columns: (a) PLATFORM DEFAULT — the canonical Mass Dynamics default "
        "(value sent if the user does nothing), with a source-of-truth citation, and "
        "(b) LLM RECOMMENDATION — what the LLM thinks the user should use given the "
        "experiment context (sample size, replicates, missingness, batch structure, "
        "gene vs protein), justified in one sentence. When the LLM recommendation "
        "diverges from the platform default, mark the row '(diverges)' and explain. "
        "Pre-fill recommended values only when explicitly labelled 'LLM "
        "recommendation, please confirm or change.'",
        "BATCH CORRECTION TECHNIQUE: When normalisation_method='batch correction', the agent "
        "MUST also specify batch_correction_technique ('limma remove batch effect' | 'combat' "
        "| 'combat seq'). 'combat seq' is gene-only. Limma takes batch_variables (list of "
        "{column,type:'categorical'}); combat / combat seq take batch_variable_combat (single "
        "column name). experiment_design is required for any technique.",
        "FILTRATION VALUES: Filtration method strings use the converter canonical (spaced) "
        "form: 'by missing values', 'by minimum abundance', 'by ptm localization probability'. "
        "Underscored aliases ('by_missing_values', 'minimum_abundance', "
        "'ptm_localization_probability') are accepted on input but deprecated.",
        "FILTRATION ENTITY MATRIX: protein → 'by missing values' only; peptide → 'by missing "
        "values' or 'by ptm localization probability'; gene → 'by minimum abundance' only; "
        "ptm → 'by missing values' or 'by ptm localization probability'; "
        "metabolite → 'by missing values' (NI for metabolite is currently upstream-"
        "gated by md-converter and may 422). Cross-entity combinations are rejected "
        "client-side.",
        "FILTER-ONLY PATTERN: To filter without normalising/imputing, pass "
        "normalisation_method='skip' + imputation_method='skip' + a filtration_method "
        "(see Workflow K). Output is still an INTENSITY dataset.",
        "DE METHOD SCOPE: Pairwise and ANOVA accept de_method ∈ {limma, edgeR, DESeq2} "
        "for entity_type='gene' only. Every other entity_type (protein, peptide, "
        "metabolite, ptm) is limma-only — the MCP rejects edgeR/DESeq2 for them "
        "client-side. Wire field is entity-keyed: ``de_method_<entity_type>``. "
        "edgeR carries `edger_norm_method`; DESeq2 carries `deseq2_lfc_shrinkage`, "
        "`deseq2_alpha`, and `apeglm_seed` (only when shrinkage='apeglm').",
        "GENE UPLOAD SOURCE: Gene-level uploads use source='md_format_gene' (long format "
        "with GeneExpression + Imputed columns). Other md_format sources are protein- or "
        "peptide-level.",
        "md_format IMPUTED FLAG (PROTEIN/PEPTIDE only): Every row where ProteinIntensity "
        "or PeptideIntensity = 0.0 MUST have Imputed=1. A zero with Imputed=0 is treated "
        "as a real measurement and causes downstream pairwise jobs to fail. If source "
        "data uses 0.0 for missing, run: "
        "long_df.loc[long_df['ProteinIntensity'] == 0, 'Imputed'] = 1. "
        "GENE EXCEPTION: For md_format_gene the converter auto-derives Imputed at "
        "md_format_gene/reader.py:120-124 (NaN or 0 → Imputed=1) so user-supplied "
        "Imputed columns are NOT required and may be overwritten.",
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
