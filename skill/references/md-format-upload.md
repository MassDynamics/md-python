# MD Format Detection and Upload

## Why this matters

MD format is the platform's native data format. If you upload MD format data
with the wrong source type (e.g. `generic_format`), the experiment will hang
in "processing" indefinitely with no error message. This is the single most
common upload failure — detecting it correctly saves hours of debugging.

## How to detect MD format

Read the TSV file headers. MD format files have these specific columns:

**Protein file** (e.g. `Protein_data.tsv`, `Protein_intensity.tsv`):
`ProteinGroupId | ProteinIntensity | Imputed | SampleName | ProteinGroup | ProteinNames | GeneNames | Description`

**Peptide file** (e.g. `Peptide_data.tsv`, `Peptide_intensity.tsv`):
`PeptideIntensity | Imputed | SampleName | ProteinGroup | ProteinNames | GeneNames | Description | ModifiedSequence | StrippedSequence | Unique | ProteinGroupId | OtherProteinGroupIDs`

If you see columns like `ProteinGroupId`, `ProteinIntensity`, `Imputed`, and
`SampleName` together — this is MD format. Use `--source md_format`.

The CLI auto-detects MD format when `--source` is omitted and `--files-dir`
is provided. It reads TSV headers and silently switches to `md_format`.

## MD format vs generic_format

| Feature | `md_format` | `generic_format` |
|---------|-------------|------------------|
| Source type | `md_format` or `md_format_gene` | `generic_format` |
| Protein columns | 8 specific columns (see above) | Flexible |
| Peptide columns | 12 specific columns (see above) | Flexible |
| Data completeness | Every protein/peptide must appear in every sample | Sparse OK |
| Missing values | Fill with `0` intensity and `0` imputed | N/A |
| Design CSV filename | `filename` = sample_name (no raw files) | `filename` = actual file path |

## MD format rules

1. **Source type**: `md_format` (protein accession-keyed) or `md_format_gene` (gene name-keyed)
2. **Complete matrix**: Every protein needs a row for every sample. If a protein was not detected in a sample, add a row with `ProteinIntensity=0` and `Imputed=0`. Same for peptides. Incomplete matrices cause the experiment to hang in processing with no error.
3. **Column order**: Follow the exact column order shown above
4. **ProteinNames and Description**: Include even if empty
5. **OtherProteinGroupIDs** (peptide): Include the ProteinGroupId followed by a semicolon (e.g. `1234;`)
6. **Design CSV**: The `filename` column contains the sample name (not a file path), since MD format data has no per-sample raw files. The CLI auto-adjusts this when source is `md_format`.
7. **Files needed**: Two TSV files (one protein, one peptide). Metadata via `--metadata-csv`.

## Zero-filling incomplete data

When data comes from search engines (DIA-NN, MaxQuant, etc.) and is converted
to MD format, proteins/peptides are typically only present for samples where
they were detected. You need to zero-fill before uploading:

```python
# Zero-fill protein data
# 1. Read all existing (protein, sample) pairs
# 2. Get the full set of unique proteins and unique samples
# 3. For every (protein, sample) pair not in the original data:
#    -> Add a row with ProteinIntensity=0, Imputed=0
# 4. Write output in MD format column order

# Same process for peptide data — every (peptide, sample) must exist
```

## Example upload

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

## Large file uploads (>100MB)

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
