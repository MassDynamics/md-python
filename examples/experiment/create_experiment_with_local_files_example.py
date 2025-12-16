"""
Example of creating an experiment using local files with the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import Experiment, ExperimentDesign, MDClient, SampleMetadata

load_dotenv()


def create_experiment_with_local_files_example():
    """Example of creating an experiment with local file uploads"""

    client = MDClient(api_token=os.getenv("API_TOKEN"))

    experiment_design = ExperimentDesign(
        data=[["filename", "sample_name", "condition"], ["file.d", "sample1", "zzz"]]
    )

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

    file_location = "/path/to/your/local/files"

    # Create the experiment with local files using file_location
    experiment = Experiment(
        name="YOUR_EXPERIMENT_NAME",
        description="Experiment description",
        experiment_design=experiment_design,
        labelling_method="lfq",
        source="raw",
        file_location=file_location,
        filenames=[
            "HE_report.log.txt",
            "HE_report.pg_matrix.tsv",
            "HE_report.pr_matrix.tsv",
        ],
        sample_metadata=sample_metadata,
    )

    try:
        experiment_id = client.experiments.create(experiment)
        print("Experiment created successfully!")
        print(f"Experiment ID: {experiment_id}")
        print("Files have been uploaded automatically.")
    except Exception as e:
        print("Error creating experiment!")
        print(e)


if __name__ == "__main__":
    create_experiment_with_local_files_example()

