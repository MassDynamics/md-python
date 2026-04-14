"""
Example of downloading a dataset table using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def download_table_example():
    """Example of getting a presigned URL for a dataset table download"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Dataset ID and table name (replace with actual values)
    dataset_id = "YOUR_DATASET_ID"
    table_name = "YOUR_TABLE_NAME"

    # Get a CSV download URL
    try:
        url = client.datasets.download_table_url(dataset_id, table_name, format="csv")
        print(f"CSV download URL: {url}")
    except Exception as e:
        print(f"Error getting CSV download URL: {e}")

    # Get a Parquet download URL
    try:
        url = client.datasets.download_table_url(dataset_id, table_name, format="parquet")
        print(f"Parquet download URL: {url}")
    except Exception as e:
        print(f"Error getting Parquet download URL: {e}")


if __name__ == "__main__":
    download_table_example()
