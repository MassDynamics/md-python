"""
Example of deleting an upload using the MD Python client
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

load_dotenv()


def delete_upload_example():
    """Example of deleting an upload by ID"""

    # Initialize client (replace with your actual API token)
    client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Upload ID to delete (replace with actual upload ID)
    upload_id = "YOUR_UPLOAD_ID"

    # Delete the upload
    try:
        success = client.uploads.delete(upload_id)
        if success:
            print(f"Upload {upload_id} deleted successfully!")
        else:
            print(f"Failed to delete upload {upload_id}")
    except Exception as e:
        print(f"Error deleting upload: {e}")


if __name__ == "__main__":
    delete_upload_example()
