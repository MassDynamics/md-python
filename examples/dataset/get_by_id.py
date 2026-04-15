"""
Example of getting a dataset by ID using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def get_dataset_by_id_example():
    """Example of getting a single dataset by ID"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Dataset ID to retrieve (replace with actual dataset ID)
    dataset_id = "YOUR_DATASET_ID"

    try:
        dataset = client.datasets.get_by_id(dataset_id)
        if dataset:
            print(f"Dataset found: {dataset.name}")
            print(f"State: {dataset.state}")
            print(dataset)
        else:
            print(f"Dataset {dataset_id} not found")
    except Exception as e:
        print(f"Error getting dataset: {e}")


if __name__ == "__main__":
    get_dataset_by_id_example()
