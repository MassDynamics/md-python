[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue?logo=apache)](https://github.com/MassDynamics/md-python/blob/main/LICENSE)

# MD Python Client

A Python client for the Mass Dynamics API that provides a simple and type-safe interface for managing experiments and datasets.

## Installation

```bash
pip install git+https://github.com/MassDynamics/md-python.git
```

## Available Resources

- **Experiments**: Create, retrieve, and update experiments
- **Datasets**: Create, retrieve, retry and delete datasets
- **Health**: Check API health status

## Quick Start

```python
from md_python import MDClient, Experiment, Dataset, SampleMetadata, ExperimentDesign

# Initialise client
client = MDClient(api_token="your_api_token")

# Check API health
health_status = client.health.check()

# Create an experiment
sample_metadata = SampleMetadata.from_csv("sample_metadata.csv")
experiment = Experiment(
    name="My Experiment",
    description="Test experiment",
    sample_metadata=sample_metadata
)
experiment_id = client.experiments.create(experiment)

# Get experiment by name
exp = client.experiments.get_by_name("My Experiment")

# Get experiment by ID
exp = client.experiments.get_by_id(experiment_id)

# Update experiment sample metadata
sample_metadata = SampleMetadata(data=[
        ["sample_name", "dose"],
        ["1", "1"],
        ["2", "20"],
])
success = client.experiments.update_sample_metadata(experiment_id, sample_metadata)


# Create a new dataset
from uuid import UUID
new_dataset = Dataset(
    input_dataset_ids=[UUID("existing-dataset-id")],
    name="Processed Data",
    job_slug="data_processing",
    job_run_params={"parameter1": "value1", "parameter2": "value2"}
)
dataset_id = client.datasets.create(new_dataset)

# Retry a failed dataset
success = client.datasets.retry(dataset_id)

# Delete a dataset
deleted = client.datasets.delete(dataset_id)

# List all datasets for an experiment
experiment_datasets = client.datasets.list_by_experiment(experiment_id)
```

## Examples

Comprehensive examples demonstrating how to use the MD Python client are available in the `examples/` directory:

- **Experiment Examples** (`examples/experiment/`):
  - Create experiments
  - Retrieve experiments by ID or name
  - Update sample metadata

- **Dataset Examples** (`examples/dataset/`):
  - Create datasets
  - Delete datasets
  - Retry failed datasets
  - List datasets by experiment

- **Health Examples** (`examples/health/`):
  - Check API health status

## Development

```bash
# Clone the repository
git clone https://github.com/MassDynamics/md-python.git
cd md-python

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run type checking
mypy .

# Format code with Black
black .

# Sort imports
isort .
```

### Using Custom Base URL

When developing or testing against an environment, you can specify a custom base URL:

```python
from md_python import MDClient

client = MDClient(
    api_token="your_api_token",
    base_url="https://xxx.massdynamics-example-installation.com/api"
)
```
