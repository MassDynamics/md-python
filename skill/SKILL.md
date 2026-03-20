---
name: md-platform
metadata:
  version: "2.0.0"
  api_version: v2
  sdk: md-python
description: >
  Mass Dynamics proteomics platform interface via the `md` CLI (v2 API).
  Use for: uploading data (DIA-NN, MaxQuant, Spectronaut), creating experiments,
  running analyses (pairwise, dose-response, ANOVA), downloading results,
  generating plots (volcano, heatmap, PCA, box plot), pathway enrichment
  (Reactome, STRING), and workspace management. Trigger on: Mass Dynamics,
  MD platform, proteomics, DIA-NN, MaxQuant, Spectronaut, experiment design,
  pairwise comparison, dose-response, ANOVA, volcano plot, heatmap, PCA,
  enrichment, Reactome, STRING, protein list, intensity dataset, or the `md`
  CLI tool. Even if the user just says "run a differential expression analysis"
  or "upload my proteomics data" — this skill should activate.
---

# md-platform — Mass Dynamics Platform Skill (v2)

This skill operates the Mass Dynamics proteomics platform via the `md` CLI.
It contains no operational rules itself — it orchestrates a workflow that loads
the right guidance at each step.

## Setup

Before any operations, ensure the CLI is installed and authenticated:

```bash
pip install -e <md-python-path>/cli --break-system-packages 2>/dev/null
md auth login --token <TOKEN>
md health
```

## Workflow

### Step 1: Understand the request

What does the user want to do? Map to the right operation:

→ Read `instructions/request-mapping.md`

### Step 2: Use batch for multi-step operations

If the task requires 2+ API calls, always use `md batch`:

→ Read `instructions/batch-usage.md`

### Step 3: Check method before running analysis

If the user wants to run a statistical analysis, validate with the policy skill first:

→ Hand off to **md-analysis-policy** skill for validation

### Step 4: Execute

→ Read `references/command-reference.md` for the exact command syntax

### Step 5: Review examples

→ Read relevant file from `examples/good/` before composing commands
→ Read `examples/bad/common-mistakes.md` to avoid known issues

### Step 6: Quality check

→ Read `eval/checklist.md` before presenting results

## Quick Reference

| Task | Command |
|---|---|
| Health check | `md health` |
| Get experiment | `md uploads get <ID>` or `md uploads get "Name" --by-name` |
| List datasets | `md datasets list <UPLOAD_ID>` |
| Find intensity dataset | `md datasets find-initial <UPLOAD_ID>` |
| Run pairwise (limma) | `md analysis pairwise --input-dataset-id <ID> ...` |
| Run multiple operations | `md batch "cmd1" "cmd2" --output results.json` |
| List analysis types | `md jobs` |

## v2 API Note

This skill uses the v2 API where experiments are called "uploads." The CLI
routes `uploads get` to `/uploads/:id` and uses the `application/vnd.md-v2+json`
accept header. The `experiments get` command in batch mode is backward-compatible
and routes to the same v2 endpoint.

## Skill Integration

- **md-analysis-policy**: Validate data and method before running analyses
- **md-custom-r**: Scaffold custom R modules when native analyses aren't sufficient
- **ddipy**: Search public omics datasets

## Reference Materials

→ `references/command-reference.md` — Full CLI command syntax
→ `references/api-v2-mapping.md` — v1 to v2 endpoint changes
→ `references/output-types.md` — Dataset types and viz compatibility
