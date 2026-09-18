"""
Manage organisation-scoped reference data with the MD Python client.

Reference data files are stored per organisation and can be listed and
uploaded independently of any upload or dataset. ``reference_data.upload``
drives the whole flow — request a presigned URL, transfer the bytes (single
PUT for small files, multipart for large ones) and finalise multipart uploads
— and returns the reference data ``id`` (``"<org_uuid>/<file_uuid>"``) that
groups the file.
"""

from md_python import MDClient

# --- Hardcode your credentials / target here -------------------------------
API_TOKEN = ""
BASE_URL = "https://dev.massdynamics.com/api"
FILE_PATH = "/Users/runtime_metadata2.csv"
# ---------------------------------------------------------------------------


def main() -> None:
    client = MDClient(api_token=API_TOKEN, base_url=BASE_URL)

    # Upload a local file as reference data (handles single vs multipart).
    uploaded = client.reference_data.upload(FILE_PATH)
    print(f"Uploaded reference data: id={uploaded.id} filename={uploaded.filename}")

    # List everything the current organisation has uploaded.
    files = client.reference_data.list()
    print(f"Found {len(files)} reference data file(s):")
    for entry in files:
        print(f"  {entry.id}: {entry.filename}")


if __name__ == "__main__":
    main()
