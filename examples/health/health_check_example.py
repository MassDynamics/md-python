"""
Example of checking the health of the API
"""

import os

from dotenv import load_dotenv

from md_python import MDClient

# Load environment variables from .env file
load_dotenv()


def main():
    # Create client instance
    md_client = MDClient(api_token=os.getenv("API_TOKEN"))

    # Health check
    health_status = md_client.health.check()

    print("Health status:")
    print(health_status)


if __name__ == "__main__":
    main()
