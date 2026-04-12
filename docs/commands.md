# Command Reference

## md health

Check API health status.

```bash
md health
# {"status": "ok"}
```

## md auth

### md auth login

```bash
md auth login --token YOUR_JWT_TOKEN
md auth login --token YOUR_TOKEN --base-url https://app.massdynamics.com/api
```

### md auth status

```bash
md auth status
# ✓ Authenticated
```

## md batch

Run multiple commands in a single invocation. Reuses one authenticated HTTP
session. Most efficient way to do multi-step operations.

```bash
md batch "cmd1" "cmd2" "cmd3" [--output file.json] [--stop-on-error]
```

**Options:**
- `--output`, `-o`: Save results to JSON file
- `--stop-on-error`: Halt on first failure

**Supported batch commands:**
- `health`
- `auth status`
- `uploads get <id>` / `uploads get <name> --by-name`
- `datasets list <upload_id>`
- `datasets get <id> -e <upload_id>`
- `datasets find-initial <upload_id>`
- `jobs`

**Example:**
```bash
md batch \
  "health" \
  "uploads get 4e48846a --by-name" \
  "datasets list 4e48846a" \
  --output results.json
```

## md uploads

Upload management. In v2 API, "uploads" replaces "experiments."

### md uploads get

```bash
md uploads get <UUID>
md uploads get "Experiment Name" --by-name
```

### md uploads wait

```bash
md uploads wait <UUID> [--timeout 600] [--interval 10]
```

## md datasets

### md datasets list

```bash
md datasets list <UPLOAD_ID>
```

### md datasets find-initial

Find the INTENSITY dataset for an upload (needed for downstream analyses).

```bash
md datasets find-initial <UPLOAD_ID>
```

### md datasets wait

```bash
md datasets wait <UPLOAD_ID> <DATASET_ID> [--timeout 600]
```

## md analysis

### md analysis pairwise

Run pairwise comparison (limma differential expression).

```bash
md analysis pairwise \
  --input-dataset-id <INTENSITY_DATASET_ID> \
  --name "Treatment vs Control" \
  --sample-metadata design.csv \
  --condition-column condition \
  --comparisons "Treatment:Control"
```

**CSV format** for `--sample-metadata`:
```
sample_name,condition
S1,Control
S2,Control
S3,Treatment
S4,Treatment
```

**Comparisons format**: `CaseGroup:ControlGroup`, comma-separated for multiple.

## md jobs

List available analysis job types.

```bash
md jobs
```
