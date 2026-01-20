"""
File upload for the MD Python client
"""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

if TYPE_CHECKING:
    from .client import MDClient


class Uploads:
    """File upload for the MD Python client"""

    def __init__(self, client: "MDClient"):
        self._client = client

    def _get_file_path(self, file_location: str, filename: str) -> str:
        """File path from location and filename

        Args:
            file_location: Local directory path where files are located
            filename: Name of the file

        Returns:
            Full path to the file
        """
        return os.path.join(file_location, filename)

    def _validate_file_exists(self, file_path: str) -> None:
        """Validate that a file exists

        Args:
            file_path: Full path to the file

        Raises:
            FileNotFoundError: If file does not exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes

        Args:
            file_path: Full path to the file

        Returns:
            File size in bytes
        """
        return os.path.getsize(file_path)

    def should_use_multipart(self, file_size: int) -> bool:
        """Determine if a file should use multipart upload based on size

        Args:
            file_size: File size in bytes

        Returns:
            True if file should use multipart upload, False otherwise
        """
        return file_size >= 31_457_280

    def file_sizes_for_api(
        self, filenames: List[str], file_location: str
    ) -> List[Optional[int]]:
        """Get file sizes formatted for API payload

        Returns None for files that should use simple upload (under 30MB),
        and the actual size for files that need multipart upload.

        Args:
            filenames: List of filenames to calculate sizes for
            file_location: Local directory path where files are located

        Returns:
            List of file sizes in bytes (None for files that should use simple upload)

        Raises:
            FileNotFoundError: If any file is not found
        """
        file_sizes: List[Optional[int]] = []
        for filename in filenames:
            file_path = self._get_file_path(file_location, filename)
            self._validate_file_exists(file_path)
            file_size = self._get_file_size(file_path)
            if self.should_use_multipart(file_size):
                file_sizes.append(file_size)
            else:
                file_sizes.append(None)
        return file_sizes

    def upload_single_file(self, url: str, file_path: str, filename: str) -> None:
        """Upload a single file to a presigned URL

        Args:
            url: Presigned URL for upload
            file_path: Local path to the file
            filename: Name of the file being uploaded

        Raises:
            Exception: If upload fails
        """
        with open(file_path, "rb") as f:
            upload_response = requests.put(url, data=f)

        if upload_response.status_code not in [200, 204]:
            raise Exception(
                f"Failed to upload {filename}: {upload_response.status_code} - {upload_response.text}"
            )

    def upload_multipart_file(
        self, parts: List[Dict[str, Any]], file_path: str, filename: str
    ) -> List[Dict[str, Any]]:
        """Upload a file using multipart upload

        Args:
            parts: List of part dictionaries containing url and part_number
            file_path: Local path to the file
            filename: Name of the file being uploaded

        Returns:
            List of part responses with ETag headers

        Raises:
            Exception: If upload fails
        """
        file_size = self._get_file_size(file_path)
        num_parts = len(parts)
        base_chunk_size = file_size // num_parts
        remainder = file_size % num_parts

        uploaded_parts = []
        with open(file_path, "rb") as f:
            for part in sorted(parts, key=lambda x: x["part_number"]):
                part_number = part["part_number"]
                url = part["url"]

                is_last_part = part_number == num_parts
                chunk_size = base_chunk_size + (remainder if is_last_part else 0)

                chunk_data = f.read(chunk_size)
                upload_response = requests.put(url, data=chunk_data)

                if upload_response.status_code not in [200, 204]:
                    raise Exception(
                        f"Failed to upload part {part_number} of {filename}: {upload_response.status_code} - {upload_response.text}"
                    )

                etag = upload_response.headers.get("ETag", "").strip('"')
                uploaded_parts.append({"part_number": part_number, "etag": etag})

        return uploaded_parts

    def complete_multipart_upload(
        self, experiment_id: str, filename: str, upload_session_id: str
    ) -> None:
        """Complete a multipart upload

        Args:
            experiment_id: ID of the experiment
            filename: Name of the file being uploaded
            upload_session_id: Upload session ID from multipart upload initiation

        Raises:
            Exception: If completion fails
        """
        response = self._client._make_request(
            method="POST",
            endpoint=f"/experiments/{experiment_id}/uploads/complete",
            json={"filename": filename, "upload_id": upload_session_id},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to complete multipart upload for {filename}: {response.status_code} - {response.text}"
            )

    def upload_files(
        self, uploads: List[Dict[str, Any]], file_location: str, experiment_id: str
    ) -> None:
        """Upload files to presigned URLs, handling both single and multipart uploads

        Args:
            uploads: List of upload dictionaries containing filename, mode, and upload details
            file_location: Local directory path where files are located
            experiment_id: ID of the experiment (for completing multipart uploads)
        """
        for upload in uploads:
            filename = upload["filename"]
            mode = upload.get("mode", "single")
            file_path = self._get_file_path(file_location, filename)
            self._validate_file_exists(file_path)

            if mode == "multipart":
                upload_session_id = upload["upload_session_id"]
                parts = upload["parts"]
                self.upload_multipart_file(parts, file_path, filename)
                self.complete_multipart_upload(
                    experiment_id, filename, upload_session_id
                )
            else:
                url = upload["url"]
                self.upload_single_file(url, file_path, filename)
