"""
Example of retrying a failed dataset using the MD Python client
"""

from md_python import MDClient
from dotenv import load_dotenv
import os

load_dotenv()


def retry_dataset_example():
    """Example of retrying a failed dataset by ID"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Dataset ID to retry (replace with actual dataset ID)
    dataset_id = "YOUR_DATASET_ID"

    # Retry the dataset
    try:
        success = client.datasets.retry(dataset_id)
        if success:
            print(f"Dataset {dataset_id} retry initiated successfully!")
        else:
            print(f"Failed to initiate retry for dataset {dataset_id}")
    except Exception as e:
        print("Error retrying dataset!")
        print(e)


if __name__ == "__main__":
    retry_dataset_example()
