"""
Example of querying datasets using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def query_datasets_example():
    """Example of querying datasets with various filters"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Query all datasets (paginated)
    try:
        result = client.datasets.query(page=1)
        print(f"Found {len(result['data'])} datasets on page 1")
        print(f"Pagination: {result['pagination']}")
    except Exception as e:
        print(f"Error querying datasets: {e}")

    # Query datasets for a specific upload
    try:
        result = client.datasets.query(upload_id="YOUR_UPLOAD_ID")
        for ds in result["data"]:
            print(f"  {ds['id']}: {ds.get('name', 'unnamed')} ({ds.get('state', 'unknown')})")
    except Exception as e:
        print(f"Error querying datasets: {e}")

    # Query with state and type filters
    try:
        result = client.datasets.query(
            state=["COMPLETED"],
            type=["INTENSITY"],
            page=1,
        )
        print(f"Found {len(result['data'])} completed intensity datasets")
    except Exception as e:
        print(f"Error querying datasets: {e}")

    # Query with a search term
    try:
        result = client.datasets.query(search="pairwise")
        print(f"Found {len(result['data'])} datasets matching 'pairwise'")
    except Exception as e:
        print(f"Error querying datasets: {e}")


if __name__ == "__main__":
    query_datasets_example()
