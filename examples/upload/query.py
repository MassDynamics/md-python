"""
Example of querying uploads using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def query_uploads_example():
    """Example of querying uploads with various filters"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Query all uploads (paginated)
    try:
        result = client.uploads.query(page=1)
        print(f"Found {len(result['data'])} uploads on page 1")
        print(f"Pagination: {result['pagination']}")
    except Exception as e:
        print(f"Error querying uploads: {e}")

    # Query with a search term
    try:
        result = client.uploads.query(search="my experiment")
        for upload in result["data"]:
            print(f"  {upload['id']}: {upload['name']}")
    except Exception as e:
        print(f"Error querying uploads: {e}")

    # Query with status and source filters
    try:
        result = client.uploads.query(
            status=["completed"],
            source=["maxquant"],
            page=1,
        )
        print(f"Found {len(result['data'])} completed maxquant uploads")
    except Exception as e:
        print(f"Error querying uploads: {e}")

    # Query with sample metadata filters
    try:
        result = client.uploads.query(
            sample_metadata=[{"key": "condition", "value": "treated"}],
        )
        print(f"Found {len(result['data'])} uploads matching metadata filter")
    except Exception as e:
        print(f"Error querying uploads: {e}")


if __name__ == "__main__":
    query_uploads_example()
