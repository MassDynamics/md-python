## Notebook Quickstart

### 1) Environment
- Python 3.10+ recommended
- Optional conda
```bash
conda create -n md-api-python python=3.10 -y
conda activate md-api-python
```

### 2) Install
- Latest from GitHub
```bash
pip install git+https://github.com/MassDynamics/md-python.git
```
- Local dev (editable)
```bash
git clone https://github.com/MassDynamics/md-python.git
cd md-python
pip install -e ".[dev,notebook]"
```

### 3) .env
Create a `.env` in your working directory:
```env
MD_API_BASE_URL=http://localhost:3000/api   # or https://app.massdynamics.com/api
MD_AUTH_TOKEN=eyJhbGciOi...                 # token WITHOUT the 'Bearer ' prefix
```

### 4) Notebook quickstart (health check)
```python
from dotenv import load_dotenv
from IPython.display import JSON, display
from md_python import MDClient

load_dotenv()
client = MDClient()  # reads env vars
health = client.health.check()
display(JSON(health, expanded=True))
```

### 5) Load metadata from CSV (as in check_health.ipynb)
```python
import os
from md_python import ExperimentDesign, SampleMetadata

metadata_path = "/path/to/dir"
exp_design = ExperimentDesign.from_csv(os.path.join(metadata_path, "experiment_design_COMBINED.csv"))
sample_meta = SampleMetadata.from_csv(os.path.join(metadata_path, "experiment_design_COMBINED.csv"))
```

### 6) Create an experiment
```python
from md_python import Experiment

exp = Experiment(
    name="my_notebook_experiment",
    source="md_format",
    labelling_method="lfq",
    s3_bucket="md-development-test-data",
    s3_prefix="some/prefix/",
    filenames=[
        "proteomics_proteins_COMBINED.tsv",
        "proteomics_peptides_COMBINED.tsv",
    ],
    experiment_design=exp_design,
    sample_metadata=sample_meta,
)

# experiment_id = client.experiments.create(exp)
```

### 7) Wait for experiment completion
```python
# completed = client.experiments.wait_until_complete(experiment_id)
# print(completed.status)
```

### 8) Find initial dataset (INTENSITY)
```python
dataset = client.datasets.find_initial_dataset(experiment_id)
dataset_id = str(dataset.id)
```

### 9) Define pairwise comparisons (control vs others)
```python
comparisons = sample_meta.pairwise_vs_control(column="condition", control="md00001_a")
```

### 10) Build and run pairwise dataset
```python
from md_python.utils.builders import PairwiseComparisonDataset

pw = PairwiseComparisonDataset(
    input_dataset_ids=[dataset_id],
    dataset_name="Pairwise test",
    sample_metadata=sample_meta,
    condition_column="condition",
    condition_comparisons=comparisons,
)
new_dataset_id = pw.run(client)
```

### 11) Wait for dataset completion
```python
state = client.datasets.wait_until_complete(
    experiment_id=experiment_id,
    dataset_id=new_dataset_id,
)
print(state["state"])  # expects COMPLETED/FAILED/ERROR/CANCELLED
```

### Notes
- Ensure `MD_API_BASE_URL` matches where your token was issued.
- If your token includes the prefix, remove `"Bearer "` in `.env`.
- `ExperimentDesign` normalizes its header automatically to `["filename", "sample_name", "condition"]`.

