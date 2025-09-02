"""
Example of deleting a dataset using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def delete_dataset_example():
    """Example of deleting a dataset by ID"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Dataset ID to delete (replace with actual dataset ID)
    dataset_id = "YOUR_DATASET_ID"

    # Delete the dataset
    try:
        success = client.datasets.delete(dataset_id)
        if success:
            print(f"Dataset {dataset_id} deleted successfully!")
        else:
            print(f"Failed to delete dataset {dataset_id}")
    except Exception as e:
        print("Error deleting dataset!")
        print(e)


if __name__ == "__main__":
    delete_dataset_example()
