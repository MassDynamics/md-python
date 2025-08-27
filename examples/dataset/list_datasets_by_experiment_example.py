"""
Example of listing datasets by experiment using the MD Python client
"""

from md_python import MDClient
from dotenv import load_dotenv
import os

load_dotenv()


def list_datasets_by_experiment_example():
    """Example of listing datasets belonging to a specific experiment"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Example experiment ID (replace with actual experiment ID)
    experiment_id = "YOUR_EXPERIMENT_ID"

    # Get datasets for the experiment
    try:
        datasets = client.datasets.list_by_experiment(experiment_id=experiment_id)

        print(f"Found {len(datasets)} datasets for experiment {experiment_id}")
        print()

        # Display information about each dataset
        for i, dataset in enumerate(datasets, 1):
            print(f"Dataset {i}:")
            print(dataset)
            print()

    except Exception as e:
        print("Error listing datasets by experiment!")
        print(e)


if __name__ == "__main__":
    list_datasets_by_experiment_example()
