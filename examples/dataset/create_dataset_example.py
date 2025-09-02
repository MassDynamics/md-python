"""
Example of creating a dataset using the MD Python client
"""

import os
from uuid import UUID

from dotenv import load_dotenv

from md_python import Dataset, MDClient

load_dotenv()


def create_dataset_example():
    """Example of creating a dataset with the API specification"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Create the dataset
    dataset = Dataset(
        input_dataset_ids=[UUID("YOUR_INPUT_DATASET_ID")],
        name="YOUR_DATASET_NAME",
        job_slug="demo_flow",
        job_run_params={"a_string_field": "demo123", "a_or_b_enum": "A"},
    )

    # Create the dataset
    try:
        dataset_id = client.datasets.create(dataset)
        print(f"Dataset created successfully!")
        print(f"Dataset ID: {dataset_id}")
    except Exception as e:
        print("Error creating dataset!")
        print(e)


if __name__ == "__main__":
    create_dataset_example()
