"""
Example of uploading files with the AI upload agent using the MD Python client

The agent inspects the files, works out the source format and experiment
design, creates the upload and starts its workflow. You only supply the files,
a name and (optionally) a description. Requires the ``upload_agent`` feature
flag on your account.
"""

import os
import sys

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def upload_agent_example(file_location: str, filenames: list[str]):
    """Upload local files and let the upload agent create the upload"""

    client = MDClient(
        api_token=os.getenv("MD_AUTH_TOKEN"), base_url=os.getenv("MD_BASE_URL")
    )

    # Uploads the files, then waits for the agent (this can take several minutes)
    result = client.uploads.run_upload_agent(
        name="My agent upload",
        file_location=file_location,
        filenames=filenames,
        description="DIA-NN run of 12 plasma samples, treated vs control",
    )

    if result["status"] == "SUCCEEDED":
        print(f"Agent created upload {result['upload_id']} ({result['source']})")
        print(result["message"])

        # The agent has started the workflow; wait for processing to finish
        upload = client.uploads.wait_until_complete(result["upload_id"])
        print(f"Upload {upload.id} is {upload.status}")
    else:
        print(f"Agent run {result['id']} failed: {result['failure_reason']}")
        print(result["message"])


if __name__ == "__main__":
    # python examples/upload/upload_agent.py /path/to/dir report.pg_matrix.tsv report.log.txt
    upload_agent_example(sys.argv[1], sys.argv[2:])
