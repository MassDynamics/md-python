"""
List the projects you can access with the MD Python client.

Each project carries the tag names of its data collection, which can be
passed to ``Upload(tags=...)`` when creating an upload.
"""

from md_python import MDClient

# --- Hardcode your credentials / target here -------------------------------
API_TOKEN = ""
BASE_URL = "https://dev.massdynamics.com/api"
# ---------------------------------------------------------------------------


def main() -> None:
    client = MDClient(api_token=API_TOKEN, base_url=BASE_URL)

    projects = client.projects.list()
    print(f"Found {len(projects)} project(s):")
    for project in projects:
        print(project)
        print()


if __name__ == "__main__":
    main()
