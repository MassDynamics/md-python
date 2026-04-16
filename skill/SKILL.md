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
  expression analysis", "upload my proteomics data", "find this protein across
  our studies", "what's significant in this comparison", or "check if my experiment
  finished" — this skill should activate.
---

# Mass Dynamics Platform Skill

Operate the Mass Dynamics proteomics platform via the `md` CLI. The CLI handles
auth, endpoint routing, payload wrapping, and error recovery — prefer it over
writing inline Python.

## Setup

```bash
pip install <skill-path>/scripts/md-cli --break-system-packages 2>/dev/null
md auth status
```

If `md auth status` fails, see Authentication below.

## Authentication

The CLI checks for auth in order:
1. `MD_API_TOKEN` environment variable (recommended for persistent use)
2. `~/.md-cli/config.json` (set via `md auth login`)

If no token is configured, ask the user:

> "I need your Mass Dynamics API token. You can:
> 1. **Quick**: Paste your token here — get it from MD web app → Settings → API Access
> 2. **Persistent**: Set `MD_API_TOKEN` in your Claude env settings"

If they paste a token: `md auth login --token <TOKEN> && md auth status`

For enterprise: deploy via managed settings at
`/Library/Application Support/ClaudeCode/managed-settings.json`:
```json
{ "env": { "MD_API_TOKEN": "org-token", "MD_BASE_URL": "https://your-instance.massdynamics.com/api" } }
```

Base URLs: production `https://app.massdynamics.com/api`, dev `https://dev.massdynamics.com/api`

## MD Format Detection

Before uploading, check whether the user's data is MD format — the platform's
native format. Uploading MD format data with the wrong source type causes the
experiment to hang in processing indefinitely with no error, which is the single
most common upload failure.

→ **Full detection rules, column specs, zero-filling, and large-file upload**:
  Read `references/md-format-upload.md`

Quick check: if the TSV headers include `ProteinGroupId`, `ProteinIntensity`,
`Imputed`, and `SampleName` together — it's MD format. Use `--source md_format`.
The CLI auto-detects this when `--source` is omitted and `--files-dir` is provided.

## CLI Output

All commands output JSON to stdout; status messages go to stderr (clean for piping).

```bash
EXP_ID=$(md experiments create ... --format ids-only)
DS_ID=$(md datasets list $EXP_ID --type INTENSITY --format ids-only)
ANALYSIS_ID=$(md analysis pairwise ... --format ids-only)
```

## Batch — Use This for Multi-Step Workflows

`md batch` runs multiple operations in one invocation, reusing a single HTTP
session. This is roughly 24x faster than individual commands and uses fewer
tokens. If a request maps to 2+ commands, combine them.

→ See `instructions/request-mapping.md` for the full command mapping table.

```bash
md batch \
  "experiments get <ID>" \
  "datasets list <ID>" \
  "datasets list <ID> --type INTENSITY --format ids-only" \
  --output results.json
```

Use `--stop-on-error` to halt on first failure.

## Experiments

```bash
md experiments get <ID>                          # by UUID
md experiments get "Name" --by-name              # by name
md experiments create \
  --name "My Study" --source diann_tabular \
  --filenames results.tsv --design-csv design.csv \
  --metadata-csv metadata.csv --species human --files-dir ./data
md experiments wait <ID> --timeout 600           # poll until done
md experiments query --search "kinase" --status COMPLETED  # V2 search
md experiments metadata <ID>                     # sample metadata
md experiments update-metadata <ID> --metadata-csv updated.csv
md experiments delete <ID> --yes
```

**Sources**: `diann_tabular`, `diann_raw`, `tims_diann`, `maxquant`,
`spectronaut`, `msfragger`, `generic_format`, `md_format`, `md_format_gene`,
`simple`, `raw`, `unknown`
**Species**: `human`, `mouse`, `yeast`, `chinese_hamster`
**Labelling**: `lfq` (default), `tmt`

## Design Helpers

After uploading, discover actual sample names before running analysis:

```bash
md design infer <ID>                    # JSON with sample names + template
md design infer <ID> --format csv > design.csv   # editable CSV
md design infer <ID> --format ids-only  # just names
```

The output includes a `conditions_template` showing the `--conditions` format
with placeholders you can fill in.

## Datasets

```bash
md datasets list <EXP_ID>
md datasets list <EXP_ID> --type INTENSITY --format ids-only
md datasets get <DS_ID> -e <EXP_ID>     # -e required on some deployments
md datasets wait <DS_ID> --timeout 300
md datasets query --search "pairwise" --state COMPLETED   # V2 search
md datasets download-url <DS_ID> output_comparisons       # V2 presigned URL
```

`datasets get` needs `-e <EXP_ID>` because direct `GET /datasets/:id` isn't
available on all deployments — the CLI falls back to the experiment's dataset list.

## Analyses

All accept design as either `--design-csv design.csv` or inline
`--conditions "s1:Control,s2:Treatment"`. Use `md design infer` to discover
sample names first.

```bash
# Pairwise (limma differential expression)
md analysis pairwise \
  --input-dataset-id <INTENSITY_DS> --name "Tx vs Ctrl" \
  --conditions "s1:Control,s2:Control,s3:Tx,s4:Tx,s5:Tx,s6:Tx" \
  --condition-column condition --comparisons "Tx:Control"

# Dose-response (R drc curve fitting)
md analysis dose-response \
  --input-dataset-id <INTENSITY_DS> --name "Dose Response" \
  --conditions "s1:0,s2:0,s3:10,s4:100,s5:1000" \
  --control-samples s1 --control-samples s2

# ANOVA (multi-condition)
md analysis anova \
  --input-dataset-id <INTENSITY_DS> --name "ANOVA" \
  --conditions "s1:A,s2:A,s3:B,s4:B,s5:C,s6:C" \
  --condition-column condition
```

Comparison format: `"Treatment:Control"` = Treatment vs Control.
Multiple: `"Treatment:Control,Drug:Vehicle"`.

Minimum: 2 conditions, 3 replicates each.

**Multi-condition and time-course designs:** ANOVA is the correct method, but
before running, always hand off to the `md-analysis-policy` skill to validate
the design — it checks for unbalanced groups, missing replicates, and whether
the design requires a custom R module instead. Call it with the experiment ID
and sample-to-condition mapping before issuing `md analysis anova`.

### Pairwise — Data Quality (read before submitting)

**Limma always runs in log space.** The R worker applies a log transform
regardless — this is statistically required and not configurable. Any
protein with a zero intensity that passes the validity filter will fail with:
`"Entity found with 0 intensities classified as valid values. Cannot convert to the log scale."`

**Why this happens:** zeros in normalised proteomics intensities are usually
artefacts of normalisation or represent proteins not detected in a sample.
The validity filter counts them as "present" but the log step cannot handle them.

**Always check before submitting pairwise:**

1. **Look for an MNAR intensity dataset.** MNAR imputation fills zero/missing
   values with statistically modelled values drawn from the lower tail of the
   distribution, making log transform safe. Check:
   ```bash
   md datasets list <EXP_ID> | grep -i mnar
   ```
   If one exists, use it as `--input-dataset-id`. If none exists, ask the
   user whether to create one before proceeding.

2. **Default `--filter-logic` to `"all conditions"`.** The platform default
   (`"at least one condition"`) lets proteins with zeros in one condition
   slip through. `"all conditions"` requires valid values in every condition
   and is much safer with real-world proteomics data.

3. **If the job fails with the zero-intensity error**, the recovery path is:
   - Find or create the MNAR dataset (`md datasets list <EXP_ID>`)
   - Resubmit using the MNAR dataset as input with `--filter-logic "all conditions"`
   - Do not attempt to disable log transform — it is not supported and is
     statistically wrong for limma

## Tables

```bash
md tables list <EXP_ID> <DS_ID>
md tables headers <EXP_ID> <DS_ID> <TABLE>
md tables download <EXP_ID> <DS_ID> <TABLE> -o results.csv
md tables query <EXP_ID> <DS_ID> <TABLE> \
  --sql "SELECT * FROM data WHERE adj_pvalue < 0.05 ORDER BY log2fc DESC LIMIT 50"
```

### Result Table Names

| Dataset Type | Tables |
|---|---|
| INTENSITY | `Protein_Intensity`, `Protein_Metadata`, `Peptide_Intensity`, `Peptide_Metadata` |
| PAIRWISE | `output_comparisons`, `runtime_metadata` |
| DOSE_RESPONSE | `output_curves`, `output_volcanoes`, `input_drc`, `runtime_metadata` |
| ANOVA | `anova_results`, `runtime_metadata` |

Key columns in `output_comparisons`: `protein_id`, `log2fc`, `pvalue`,
`adj_pvalue`, `comparison`.

**`tables download` 404 in dev/staging:** `md tables download` can return a
404 on non-production deployments due to a missing service route. If this
happens, fall back to `md tables query` with `SELECT * FROM <TABLE>` and pipe
the output — it uses a different code path that works in all environments.

Table endpoints require session cookie auth on some deployments — the CLI gives
a clear error if only a bearer token is available.

## Visualisations

All output Plotly JSON. Save with `-o` and render with plotly.

```bash
md viz volcano --workspace-id <WS> --dataset-id <DS> --comparison "A_vs_B" -o volcano.json
md viz heatmap --workspace-id <WS> --dataset-ids <DS1> --dataset-ids <DS2>
md viz pca --workspace-id <WS> --dataset-ids <DS> --method pca
md viz box-plot --workspace-id <WS> --dataset-ids <DS> --proteins TP53 --proteins BRCA1
md viz dose-response --experiment-id <EXP> --dataset-ids <DS>
md viz anova-volcano --workspace-id <WS> --dataset-id <DS>
md viz qc --workspace-id <WS> --dataset-ids <DS> --type intensity-distribution
```

## Enrichment

**Always use the `md` CLI commands below — never express enrichment as raw HTTP
calls.** Pass a list of significant protein accessions or gene names (filtered
from pairwise results by adj_pvalue) as individual `--proteins` flags.

```bash
# Reactome pathway enrichment — one --proteins flag per protein
md enrichment reactome --experiment-id <EXP> --proteins P04637 --proteins TP53

# STRING network enrichment — requires a saved protein list ID and NCBI species taxon
md enrichment string --experiment-id <EXP> --protein-list-id <LIST_ID> --species 9606
```

Note: Reactome takes `--proteins` (direct list); STRING takes `--protein-list-id`
(a saved list object) plus mandatory `--species <NCBI_TAXON>`. These are
different input contracts — do not conflate them.

## Entities — Cross-Dataset Search (V2)

Find proteins, genes, and peptides across multiple datasets. This is the
substrate search capability — useful for validating hits against historical data
or building organisational intelligence about what's been found before.

```bash
# --keyword must be a specific gene or protein name (e.g. TP53, BRCA1, BRD4)
# Do NOT use qualitative terms like "significant" or "upregulated" as keywords
# There is NO --intersect flag — intersection logic is done client-side on results
md entities query --keyword TP53 --dataset-ids <DS1> --dataset-ids <DS2>
md entities query --keyword BRD4 --dataset-ids <DS1> --dataset-ids <DS2> --dataset-ids <DS3>
```

To discover dataset IDs across all experiments, use:
```bash
md datasets query --state COMPLETED   # list all completed datasets (V2)
```

**Flipper flag required:** The `:entity_mapping_search` feature flag must be
enabled for the user's account before `md entities query` will work — without
it you get a **403 Forbidden**. If you receive a 403, tell the user to ask a
platform admin to enable `entity_mapping_search` for their account. Do not
retry without flag enablement.

Note: the entity-mapping-service (neptune) is a separate system for
protein-to-protein relationship mapping and is not yet exposed via the API
— that capability is coming in a future release.

## Workspaces

```bash
md workspaces create --name "Analysis Workspace"
md workspaces add-experiment <WS_ID> <EXP_ID>
md workspaces datasets <WS_ID>
md workspaces tabs <WS_ID>
```

## Python API

For complex workflows where CLI flags aren't sufficient:

```python
from md_cli.api import MDClient
client = MDClient()  # reads token from config
result = client.create_experiment(name="My Experiment", source="diann_tabular", ...)
```

## Core Workflow

1. **Upload**: `md experiments create ...` (auto-uploads + starts workflow)
2. **Wait**: `md experiments wait <EXP_ID>`
3. **Discover samples**: `md design infer <EXP_ID>`
4. **Find intensity data**: `md datasets list <EXP_ID> --type INTENSITY --format ids-only`
5. **Check for MNAR dataset**: `md datasets list <EXP_ID> | grep -i mnar` — if one
   exists, use it as `--input-dataset-id` for pairwise (safer for log transform)
6. **Analyse**: `md analysis pairwise --conditions "..." --filter-logic "all conditions" ...`
7. **Wait**: `md datasets wait <ANALYSIS_DS_ID>`
8. **Results**: `md tables query ... --sql "SELECT ... WHERE adj_pvalue < 0.05"`
9. **Visualise**: `md viz volcano ...` (needs workspace)
10. **Enrich**: `md enrichment reactome ...`

If the user gives sample-to-condition mapping in natural language ("3 Control,
3 Treatment"), upload first, then use `md design infer` to get real sample names
and map the user's intent to actual identifiers. Use `--conditions` inline — no
need for a CSV file.

## API Details

- The CLI handles payload wrapping (`{"experiment": {...}}`) and array-of-arrays
  conversion for design CSVs automatically
- V2 endpoints use Bearer token auth; some V1 routes need session cookies — the
  CLI tries both and gives clear errors
- Status messages (✓, ✗) go to stderr; JSON goes to stdout

→ Full endpoint status: `references/api-v2-mapping.md`
