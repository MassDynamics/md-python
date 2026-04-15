[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue?logo=apache)](https://github.com/MassDynamics/md-python/blob/main/LICENSE)

# MD Python Client

A Python client for the Mass Dynamics API.

## Installation

```bash
pip install https://github.com/MassDynamics/md-python/archive/refs/tags/v0.2.3-31.tar.gz
```

## Quick Start

```python
from md_python import MDClient

client = MDClient(api_token="your_api_token")
```

The client defaults to the v2 API. For v1 usage, see [V1.md](V1.md).

## Resources

- **Uploads**: Create, retrieve, and manage file uploads
- **Datasets**: Create, list, retry, cancel, and delete datasets
- **Entities**: Query entity metadata across datasets
- **Jobs**: List available dataset jobs
- **Health**: Check API health status

## Uploads

Uploads replace v1 experiments. They handle file ingestion and workflow triggering.

```python
from md_python import Upload, ExperimentDesign, SampleMetadata

experiment_design = ExperimentDesign(data=[
    ["filename", "sample_name", "condition"],
    ["evidence.txt", "sample1", "control"],
    ["proteinGroups.txt", "sample2", "treated"],
])

sample_metadata = SampleMetadata(data=[
    ["sample_name", "condition"],
    ["sample1", "control"],
    ["sample2", "treated"],
])

# Create an upload from S3
upload = Upload(
    name="My Upload",
    source="maxquant",
    s3_bucket="my-bucket",
    s3_prefix="data/",
    filenames=["evidence.txt", "proteinGroups.txt"],
    experiment_design=experiment_design,
    sample_metadata=sample_metadata,
)
upload_id = client.uploads.create(upload)

# Create an upload from local files
upload = Upload(
    name="My Upload",
    source="maxquant",
    file_location="/path/to/files",
    filenames=["evidence.txt", "proteinGroups.txt"],
    experiment_design=experiment_design,
    sample_metadata=sample_metadata,
)
upload_id = client.uploads.create(upload)

# Get upload by ID
upload = client.uploads.get_by_id(upload_id)

# Get upload sample metadata
metadata = client.uploads.get_sample_metadata(upload_id)

# Update sample metadata
sample_metadata = SampleMetadata(data=[
    ["sample_name", "condition"],
    ["sample1", "control"],
    ["sample2", "treated"],
])
client.uploads.update_sample_metadata(upload_id, sample_metadata)

# Query uploads with filters and pagination
result = client.uploads.query(status=["completed"], source=["maxquant"], search="my upload", page=1)
uploads = result["data"]
pagination = result["pagination"]

# Delete an upload
client.uploads.delete(upload_id)

# Wait for upload processing to complete
upload = client.uploads.wait_until_complete(upload_id)
```

## Datasets

```python
from uuid import UUID
from md_python import Dataset

# Create a dataset
dataset = Dataset(
    input_dataset_ids=[UUID("existing-dataset-id")],
    name="Processed Data",
    job_slug="pairwise_comparison",
    job_run_params={"condition_column": "condition"},
)
dataset_id = client.datasets.create(dataset)

# Get a single dataset by ID (includes error_message)
dataset = client.datasets.get_by_id(dataset_id)

# List datasets for an upload (uses the query endpoint internally)
datasets = client.datasets.list_by_upload(upload_id)

# Query datasets with filters and pagination
result = client.datasets.query(upload_id=upload_id, state=["COMPLETED"], type=["INTENSITY"], page=1)
datasets = result["data"]
pagination = result["pagination"]

# Get a presigned URL for table download (csv or parquet)
url = client.datasets.download_table_url(dataset_id, "table_name", format="csv")

# Find the initial intensity dataset
initial = client.datasets.find_initial_dataset(upload_id)

# Retry a failed dataset
client.datasets.retry(dataset_id)

# Cancel a processing dataset
client.datasets.cancel(dataset_id)

# Delete a dataset
client.datasets.delete(dataset_id)

# Wait for a dataset to complete
ds = client.datasets.wait_until_complete(upload_id, dataset_id)
```

## Entities

```python
# Query entity metadata (proteins, genes, peptides) across datasets
result = client.entities.query(keyword="BRCA1", dataset_ids=["dataset-id-1", "dataset-id-2"])
entities = result["results"]
```

## Jobs

```python
# List available dataset jobs
jobs = client.jobs.list()
```

## Health

```python
health_status = client.health.check()
```

## Custom Base URL

```python
client = MDClient(
    api_token="your_api_token",
    base_url="https://xxx.massdynamics-example-installation.com/api",
)
```

## V1 API

For v1 API usage, pass `version="v1"` or see [V1.md](V1.md).

```python
client = MDClient(api_token="your_api_token", version="v1")
```

## Development

```bash
git clone https://github.com/MassDynamics/md-python.git
cd md-python
pip install -e ".[dev]"
pytest
```
