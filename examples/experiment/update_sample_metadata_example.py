"""
Example of updating sample metadata for an experiment using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient, SampleMetadata

load_dotenv()


def update_sample_metadata_example():
    """Example of updating sample metadata for an existing experiment"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Example experiment ID (replace with actual experiment ID)
    experiment_id = "YOUR_EXPERIMENT_ID"

    # Create new sample metadata
    new_sample_metadata = SampleMetadata(
        data=[
            ["sample_name", "dose"],
            ["1", "1"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["6", "60"],
        ]
    )

    # Update the experiment's sample metadata
    try:
        success = client.experiments.update_sample_metadata(
            experiment_id=experiment_id, sample_metadata=new_sample_metadata
        )

        if success:
            print(
                f"Sample metadata updated successfully for experiment {experiment_id}"
            )
        else:
            print("Failed to update sample metadata")

    except Exception as e:
        print("Error updating sample metadata!")
        print(e)


if __name__ == "__main__":
    update_sample_metadata_example()
