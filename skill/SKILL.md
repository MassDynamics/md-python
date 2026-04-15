---
name: md-platform
description: >
  Mass Dynamics proteomics platform interface via the `md` CLI. Use for: uploading
  data (DIA-NN, MaxQuant, Spectronaut, MD format, MSFragger), creating experiments,
  running analyses (pairwise, dose-response, ANOVA), downloading results, generating
  plots (volcano, heatmap, PCA, box plot), pathway enrichment (Reactome, STRING),
  workspace management, searching entities (proteins/genes) across datasets, and
  querying uploads/datasets with filters. Trigger on: Mass Dynamics, MD platform,
  proteomics, DIA-NN, MaxQuant, Spectronaut, MD format, MSFragger, experiment design,
  pairwise comparison, dose-response, ANOVA, volcano plot, heatmap, PCA, enrichment,
  Reactome, STRING, protein list, intensity dataset, entity search, cross-study
  comparison, or the `md` CLI tool. Even if the user just says "run a differential
  expression analysis", "upload my proteomics data", or "find this protein across
  our studies" — this skill should activate.
---

# Mass Dynamics Platform Skill

This skill operates the Mass Dynamics proteomics platform via the `md` CLI.
Always use the CLI instead of writing inline Python — it handles auth, endpoint
routing, payload wrapping, and error recovery automatically.

## Setup

Install the CLI and configure auth:

```bash
pip install <skill-path>/scripts/md-cli --break-system-packages 2>/dev/null
md auth status
```

If `md auth status` fails, see the Authentication section below.

## Authentication

The CLI checks for auth in this order:
1. `MD_API_TOKEN` environment variable (recommended for enterprise/persistent use)
2. `~/.md-cli/config.json` (set via `md auth login`)

To check current auth status: `md auth status`

### If no token is configured

Ask the user:

> "I need your Mass Dynamics API token. You can:
>
> 1. **Quick setup**: Paste your token here and I'll configure it.
>    Get your token from: MD web app → Settings → API Access → Generate Token
>
> 2. **Persistent setup** (recommended): Set `MD_API_TOKEN` in your Claude
>    settings so it's available every session automatically.
>    Open Claude settings → Add to env: `{ "MD_API_TOKEN": "your-token" }`"

### If token is provided in chat

```bash
md auth login --token <TOKEN>
md auth status  # verify
```

### For enterprise deployments

IT can deploy the token via managed settings so individual users never configure it:
- macOS: `/Library/Application Support/ClaudeCode/managed-settings.json`
- Linux: `/etc/claude-code/managed-settings.json`

```json
{ "env": { "MD_API_TOKEN": "org-token", "MD_BASE_URL": "https://your-instance.massdynamics.com/api" } }
```

### Base URLs

- Production: `https://app.massdynamics.com/api`
- Development: `https://dev.massdynamics.com/api` (CLI default)

To set a custom base URL: `md auth login --token TOKEN --base-url https://app.massdynamics.com/api`

## CRITICAL: Detecting and Uploading MD Format Data

Before uploading, ALWAYS check if the user's data is in **MD format**. This is
the most common mistake — using `generic_format` when the data is actually MD format
will cause the experiment to get stuck in "processing" indefinitely.

### How to detect MD format

Read the TSV file headers. MD format files have these specific columns:

**Protein file** (e.g. `Protein_data.tsv`, `Protein_intensity.tsv`):
`ProteinGroupId | ProteinIntensity | Imputed | SampleName | ProteinGroup | ProteinNames | GeneNames | Description`

**Peptide file** (e.g. `Peptide_data.tsv`, `Peptide_intensity.tsv`):
`PeptideIntensity | Imputed | SampleName | ProteinGroup | ProteinNames | GeneNames | Description | ModifiedSequence | StrippedSequence | Unique | ProteinGroupId | OtherProteinGroupIDs`

If you see columns like `ProteinGroupId`, `ProteinIntensity`, `Imputed`, and
`SampleName` together — **this is MD format**. Use `--source md_format`.

The CLI now auto-detects MD format when `--source` is omitted and `--files-dir`
is provided. It reads TSV headers and silently switches to `md_format`.

### MD format vs generic_format

| Feature | `md_format` | `generic_format` |
|---------|-------------|------------------|
| Source type | `md_format` or `md_format_gene` | `generic_format` |
| Protein columns | 8 specific columns (see above) | Flexible |
| Peptide columns | 12 specific columns (see above) | Flexible |
| Data completeness | **Every protein/peptide must appear in every sample** | Sparse OK |
| Missing values | Fill with `0` intensity and `0` imputed | N/A |
| Design CSV filename | `filename` = sample_name (no raw files) | `filename` = actual file path |

### MD format rules

1. **Source type**: Must be `md_format` (protein accession-keyed) or `md_format_gene` (gene name-keyed)
2. **Complete matrix required**: Every protein must have a row for every sample. If a protein was not detected in a sample, add a row with `ProteinIntensity=0` and `Imputed=0`. Same rule applies to peptides.
3. **Column order matters**: Follow the exact column order shown above
4. **ProteinNames and Description**: These columns should be present even if empty
5. **OtherProteinGroupIDs** (peptide file): Include the ProteinGroupId followed by a semicolon (e.g. `1234;`)
6. **Design CSV**: The `filename` column must contain the sample name (not a file path), since MD format data has no per-sample raw files. The CLI auto-adjusts this when `source` is `md_format`.
7. **Files needed**: Two TSV files — one protein, one peptide. Metadata is passed via `--metadata-csv`.

### Zero-filling workflow for incomplete data

When data comes from search engines (DIA-NN, MaxQuant, etc.) and is converted to
MD format, proteins/peptides are typically only present for samples where they were
detected. You MUST zero-fill before uploading:

```python
# Zero-fill protein data
# 1. Read all existing (protein, sample) pairs
# 2. Get the full set of unique proteins and unique samples
# 3. For every (protein, sample) pair not in the original data:
#    → Add a row with ProteinIntensity=0, Imputed=0
# 4. Write output in MD format column order

# Same process for peptide data — every (peptide, sample) must exist
```

### Example MD format upload

```bash
md experiments create \
  --name "My SCP Study" \
  --source md_format \
  --filenames Protein_data.tsv \
  --filenames Peptide_data.tsv \
  --design-csv design.csv \
  --metadata-csv sample_metadata.csv \
  --species human \
  --files-dir ./data
```

### Large file uploads (>100MB)

For large zero-filled peptide files, the default upload may timeout. Use the
Python API directly with a longer timeout:

```python
import json, csv, requests
from pathlib import Path

config_path = Path.home() / ".md-cli" / "config.json"
with open(config_path) as f:
    config = json.load(f)

token = config["token"]
base = config.get("base_url", "https://dev.massdynamics.com/api")
headers = {"Authorization": f"Bearer {token}",
           "Accept": "application/vnd.md-v1+json",
           "Content-Type": "application/json"}

# Create experiment
payload = {
    "experiment": {
        "name": "My Experiment",
        "source": "md_format",
        "labelling_method": "lfq",
        "file_location": "local",
        "filenames": ["Protein_data.tsv", "Peptide_data.tsv"],
        "experiment_design": design_rows,  # array-of-arrays from CSV
        "sample_metadata": meta_rows,      # array-of-arrays from CSV
        "species": "human"
    }
}
resp = requests.post(f"{base}/experiments", json=payload, headers=headers)
data = resp.json()
exp_id = data["id"]

# Upload files with long timeout (1800s for large files)
for upload in data.get("uploads", []):
    fn, url = upload["filename"], upload["url"]
    with open(f"./data/{fn}", "rb") as fh:
        requests.put(url, data=fh,
                     headers={"Content-Type": "application/octet-stream"},
                     timeout=1800)

# Start workflow
requests.post(f"{base}/experiments/{exp_id}/start_workflow", headers=headers)
```

## CLI Output Format

All commands output **JSON to stdout** by default. Status messages go to stderr
so they don't interfere with piping.

Use `--format ids-only` on key commands to get just the resource ID for piping:

```bash
EXP_ID=$(md experiments create ... --format ids-only)
DS_ID=$(md datasets list $EXP_ID --type INTENSITY --format ids-only)
ANALYSIS_ID=$(md analysis pairwise ... --format ids-only)
```

Key JSON fields in responses:
- Experiment creation → `{"id": "exp_...", "name": "...", ...}`
- Dataset creation → `{"id": "dset_...", "type": "PAIRWISE", ...}`
- Analysis submission → `{"id": "dset_...", ...}` (analyses create datasets)

## CLI Reference

### Batch Command — Use This for Multi-Step Workflows

The `md batch` command runs multiple operations in a single invocation. This is
the most efficient way to interact with the platform — it reuses one authenticated
HTTP session and avoids per-command overhead.

```bash
md batch \
  "health" \
  "auth status" \
  "experiments get <EXPERIMENT_ID>" \
  "datasets list <EXPERIMENT_ID>" \
  "datasets get <DATASET_ID> -e <EXPERIMENT_ID>" \
  --output results.json
```

Use `--stop-on-error` to halt on first failure. Always prefer `md batch` over
running individual commands when you need 2+ operations.

### Experiments

```bash
# Get experiment by UUID
md experiments get <EXPERIMENT_ID>

# Get experiment by name
md experiments get "My Experiment" --by-name

# Create experiment with data files (returns JSON with experiment ID)
md experiments create \
  --name "My DIA-NN Study" \
  --source diann_tabular \
  --filenames results.tsv \
  --design-csv design.csv \
  --metadata-csv metadata.csv \
  --species human \
  --files-dir ./data

# Create and get just the ID for piping
EXP_ID=$(md experiments create ... --format ids-only)

# Wait for processing
md experiments wait <EXPERIMENT_ID> --timeout 600

# List experiments (requires session cookie auth)
md experiments list
```

**Data sources** (all valid API values):
`diann_tabular`, `diann_raw`, `tims_diann`, `maxquant`, `spectronaut`,
`msfragger`, `generic_format`, `md_format`, `md_format_gene`, `simple`,
`raw`, `unknown`
**Species**: `human`, `mouse`, `yeast`, `chinese_hamster`
**Labelling**: `lfq` (default), `tmt`

### Design Helpers

After uploading an experiment, discover actual sample names:

```bash
# Infer sample names from uploaded data
md design infer <EXPERIMENT_ID>

# Output as CSV template for manual editing
md design infer <EXPERIMENT_ID> --format csv > design.csv

# Get just the sample names
md design infer <EXPERIMENT_ID> --format ids-only
```

The `design infer` output includes a `conditions_template` field showing the
`--conditions` string format with placeholder values you can fill in.

### Datasets

```bash
# List datasets for an experiment
md datasets list <EXPERIMENT_ID>

# Filter by type and get just IDs
DS_ID=$(md datasets list <EXPERIMENT_ID> --type INTENSITY --format ids-only)

# Get dataset details (always provide -e for reliability)
md datasets get <DATASET_ID> -e <EXPERIMENT_ID>

# Wait for analysis to complete
md datasets wait <DATASET_ID> --timeout 300
```

Important: `datasets get` requires the `-e <EXPERIMENT_ID>` flag because the
direct `GET /datasets/:id` route is not available on all deployments. The CLI
falls back to looking up the dataset from the experiment's dataset list.

### Analyses

All analysis commands accept design in two ways:
- `--design-csv design.csv` — CSV file with sample_name and condition columns
- `--conditions "sample1:Control,sample2:Treatment"` — inline, no file needed

Use `md design infer <EXP_ID>` to discover sample names first.

```bash
# Pairwise comparison (differential expression via limma)
md analysis pairwise \
  --input-dataset-id <INTENSITY_DATASET_ID> \
  --name "Treatment vs Control" \
  --conditions "s1:Control,s2:Control,s3:Control,s4:Tx,s5:Tx,s6:Tx" \
  --condition-column condition \
  --comparisons "Tx:Control"

# Same thing with a CSV file
md analysis pairwise \
  --input-dataset-id <INTENSITY_DATASET_ID> \
  --name "Treatment vs Control" \
  --design-csv design.csv \
  --condition-column condition \
  --comparisons "Treatment:Control"

# Get just the analysis dataset ID
ANALYSIS_ID=$(md analysis pairwise ... --format ids-only)

# Dose-response (curve fitting via R drc)
md analysis dose-response \
  --input-dataset-id <INTENSITY_DATASET_ID> \
  --name "Dose Response" \
  --conditions "s1:0,s2:0,s3:10,s4:100,s5:1000" \
  --control-samples s1 --control-samples s2

# ANOVA (multi-condition)
md analysis anova \
  --input-dataset-id <INTENSITY_DATASET_ID> \
  --name "ANOVA Analysis" \
  --conditions "s1:A,s2:A,s3:B,s4:B,s5:C,s6:C" \
  --condition-column condition
```

Comparison string format for pairwise: `"Treatment:Control"` means Treatment vs Control.
Multiple comparisons: `"Treatment:Control,Drug:Vehicle"`.

Minimum requirements: 2 distinct conditions, 3 replicates per condition.
Before running analysis, consider handing off to md-analysis-policy for validation.

### Tables

```bash
# List available tables for a dataset
md tables list <EXPERIMENT_ID> <DATASET_ID>

# Column headers
md tables headers <EXPERIMENT_ID> <DATASET_ID> <TABLE_NAME>

# Download as CSV
md tables download <EXPERIMENT_ID> <DATASET_ID> <TABLE_NAME> -o results.csv

# SQL query
md tables query <EXPERIMENT_ID> <DATASET_ID> <TABLE_NAME> \
  --sql "SELECT * FROM data WHERE adj_pvalue < 0.05 ORDER BY log2fc DESC LIMIT 50"
```

#### Result Table Names (use `md tables list` if unsure)

| Dataset Type     | Tables                                                                     |
|------------------|----------------------------------------------------------------------------|
| **INTENSITY**    | `Protein_Intensity`, `Protein_Metadata`, `Peptide_Intensity`, `Peptide_Metadata` |
| **PAIRWISE**     | `output_comparisons`, `runtime_metadata`                                   |
| **DOSE_RESPONSE**| `output_curves`, `output_volcanoes`, `input_drc`, `runtime_metadata`       |
| **ANOVA**        | `anova_results`, `runtime_metadata`                                        |

#### Key Column Names in `output_comparisons` (Pairwise Results)

| Column       | Description                               |
|--------------|-------------------------------------------|
| `protein_id` | Protein identifier (UniProt accession)    |
| `log2fc`     | Log2 fold change                          |
| `pvalue`     | Raw p-value                               |
| `adj_pvalue` | Adjusted p-value (Benjamini-Hochberg)     |
| `comparison` | Which comparison (e.g. "Treatment_vs_Control") |

Table endpoints require session cookie auth on some deployments. If you only
have a bearer token, the CLI will give a clear error message explaining this.

### Visualisations

All viz commands output Plotly JSON. Save with `-o` and render with plotly.

```bash
md viz volcano --workspace-id <WS> --dataset-id <DS> --comparison "A_vs_B" -o volcano.json
md viz heatmap --workspace-id <WS> --dataset-ids <DS1> --dataset-ids <DS2>
md viz pca --workspace-id <WS> --dataset-ids <DS> --method pca
md viz box-plot --workspace-id <WS> --dataset-ids <DS> --proteins TP53 --proteins BRCA1
md viz dose-response --experiment-id <EXP> --dataset-ids <DS>
md viz anova-volcano --workspace-id <WS> --dataset-id <DS>
md viz qc --workspace-id <WS> --dataset-ids <DS> --type intensity-distribution
```

### Enrichment

```bash
md enrichment reactome --experiment-id <EXP> --proteins P04637 --proteins BRCA1_HUMAN
md enrichment string --experiment-id <EXP> --protein-list-id <LIST_ID> --species 9606
```

### Experiment Search & Management (V2 API)

```bash
# Search experiments/uploads by keyword
md experiments query --search "TPD screen"

# Filter by status and source
md experiments query --status COMPLETED --source diann_tabular

# Search and get just IDs for piping
md experiments query --search "kinase" --format ids-only

# Get sample metadata for an experiment
md experiments metadata <EXPERIMENT_ID>

# Delete an experiment
md experiments delete <EXPERIMENT_ID> --yes
```

### Dataset Search & Downloads (V2 API)

```bash
# Search datasets across all uploads
md datasets query --search "pairwise" --state COMPLETED

# Filter by upload and type
md datasets query --upload-id <UUID> --type PAIRWISE --format ids-only

# Get a presigned download URL for a table (for large downloads)
md datasets download-url <DATASET_ID> output_comparisons
md datasets download-url <DATASET_ID> Protein_Intensity --format parquet
```

### Entities — Cross-Dataset Search (V2 API)

The entities endpoint is the substrate search capability — find proteins, genes,
and peptides across multiple datasets. Use this for:
- Validating hits from a new screen against historical work
- Comparing the same protein across multiple studies
- Building organisational intelligence about what's been found before

```bash
# Search for a protein across datasets
md entities query --keyword TP53 --dataset-ids <DS1> --dataset-ids <DS2>

# Validate hits from a new TPD screen
md entities query --keyword BRCA1 --dataset-ids <NEW_DS> --dataset-ids <HISTORICAL_DS>
```

### Workspaces

```bash
md workspaces create --name "Analysis Workspace"
md workspaces add-experiment <WORKSPACE_ID> <EXPERIMENT_ID>
md workspaces datasets <WORKSPACE_ID>
md workspaces tabs <WORKSPACE_ID>
```

## Using the Python API Directly

For complex workflows where CLI flags aren't sufficient, import the client:

```python
from md_cli.api import MDClient

client = MDClient()  # reads token from config

# Example: create experiment programmatically
result = client.create_experiment(
    name="My Experiment",
    source="diann_tabular",
    filenames=["results.tsv"],
    experiment_design=[["filename","sample_name","condition"], ["results.tsv","S1","Control"]],
    sample_metadata=[["sample_name","condition"], ["S1","Control"]],
    species="human",
)
```

## Core Workflow

The typical MD workflow:

1. **Create & upload**: `md experiments create ...` (auto-uploads files + starts workflow)
2. **Wait**: `md experiments wait <EXP_ID>`
3. **Discover samples**: `md design infer <EXP_ID>` → get real sample names
4. **Find intensity dataset**: `md datasets list <EXP_ID> --type INTENSITY --format ids-only`
5. **Run analysis**: `md analysis pairwise --conditions "..." ...` (or dose-response, anova)
6. **Wait for analysis**: `md datasets wait <ANALYSIS_DATASET_ID>`
7. **Download results**: `md tables query ... --sql "SELECT ... WHERE adj_pvalue < 0.05"`
8. **Visualise**: `md viz volcano ...` (needs workspace)
9. **Enrich**: `md enrichment reactome ...`

### Working with Experiment Design

If the user provides sample-to-condition mapping in natural language
(e.g. "3 Control, 3 Treatment"):

1. Upload the experiment first (`md experiments create`)
2. After processing completes, run `md design infer <EXP_ID>` to get actual sample names
3. Map the user's intent to the real sample identifiers
4. Use `--conditions` flag inline — no need to create a CSV file:
   `md analysis pairwise --conditions "realname1:Control,realname2:Treatment" ...`

If the user provides a design.csv file, use `--design-csv` as before.

Minimum requirements: 2 conditions, 3 replicates per condition.

## Important API Details

- **Payload wrapping**: The CLI handles `{"experiment": {...}}` and `{"dataset": {...}}` wrapping automatically
- **Array-of-arrays format**: Design CSVs are converted to the required format by the CLI
- **Accept headers**: The CLI sends the correct `application/vnd.md-v1+json` headers
- **Web routes vs API routes**: Some endpoints (experiments list, workspaces, tables) are web routes requiring session cookies. The CLI tries both routes and gives clear error messages
- **Status messages vs data**: Status messages (✓, ✗, progress) go to stderr. JSON data goes to stdout. This means `--format ids-only` output is clean for piping.

## Reference

For the complete API endpoint listing:
→ Read `references/API_ENDPOINTS.md`
