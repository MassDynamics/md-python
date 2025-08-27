"""
Example of getting an experiment by name using the MD Python client
"""

from md_python import MDClient
from dotenv import load_dotenv
import os

load_dotenv()


def main():
    # Initialize the client with your API token
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Example experiment name (replace with an actual experiment name)
    experiment_name = "YOUR_EXPERIMENT_NAME"

    try:
        # Get the experiment by name
        experiment = client.experiments.get_by_name(experiment_name)

        print("Experiment found!")
        print(experiment)

    except Exception as e:
        print("Error retrieving experiment!")
        print(e)


if __name__ == "__main__":
    main()
