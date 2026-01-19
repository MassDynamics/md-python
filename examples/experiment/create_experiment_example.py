"""
Example of creating an experiment using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import Experiment, ExperimentDesign, MDClient, SampleMetadata

load_dotenv()


def create_experiment_example():
    """Example of creating an experiment with the API specification"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Create experiment design metadata
    experiment_design = ExperimentDesign(
        data=[["filename", "sample_name", "condition"], ["file.d", "sample1", "zzz"]]
    )

    # Create sample metadata
    sample_metadata = SampleMetadata(
        data=[
            ["sample_name", "dose"],
            ["sample1", "1"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["6", "60"],
        ]
    )

    # Create the experiment
    experiment = Experiment(
        name="YOUR_EXPERIMENT_NAME",
        description="Experiment description",
        experiment_design=experiment_design,
        labelling_method="lfq",
        source="diann_tabular",
        s3_bucket="s3-bucket",
        s3_prefix="s3-prefix",
        filenames=[
            "HE_report.log.txt",
            "HE_report.pg_matrix.tsv",
            "HE_report.pr_matrix.tsv",
        ],
        sample_metadata=sample_metadata,
    )

    # Create the experiment
    try:
        experiment_id = client.experiments.create(experiment)
        print(f"Experiment created successfully!")
        print(f"Experiment ID: {experiment_id}")
    except Exception as e:
        print("Error creating experiment!")
        print(e)


if __name__ == "__main__":
    create_experiment_example()
