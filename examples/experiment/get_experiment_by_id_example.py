"""
Example of getting an experiment by ID using the MD Python client
"""

from md_python import MDClient
from dotenv import load_dotenv
import os

load_dotenv()


def main():
    # Initialize the client with your API token# Replace with your actual API token
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Example experiment ID (replace with an actual experiment ID)
    experiment_id = "YOUR_EXPERIMENT_ID"

    try:
        # Get the experiment by ID
        experiment = client.experiments.get_by_id(experiment_id)

        print(f"Experiment found!")
        print(experiment)

    except Exception as e:
        print(f"Error retrieving experiment: {e}")


if __name__ == "__main__":
    main()
