"""
Reference data resource for the MD Python v2 client.

Reference data files are organisation-scoped and stored in S3 under
``reference_data/upload/<org_uuid>/<file_uuid>/<filename>``. Each
:meth:`ReferenceData.upload` call stores one file and returns its ``id``
(``"<org_uuid>/<file_uuid>"``); :meth:`ReferenceData.list` enumerates them.
"""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...uploads import Uploads as FileUploader

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class ReferenceData:
    """V2 reference data resource — organisation-scoped file uploads."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client
        # Reuse the shared presigned-URL byte transfer helpers. The complete
        # step is handled directly on this resource (the endpoint expects
        # ``upload_session_id`` rather than the ``upload_id`` key
        # FileUploader.complete_multipart_upload sends), so only the transfer
        # helpers on this uploader are used.
        self._uploader = FileUploader(
            client,
            resource_path="/reference_data",
            complete_path="/complete",
        )

    def create_upload(self, filename: str, file_size: int) -> Dict[str, Any]:
        """Request a presigned S3 upload URL for a single reference data file.

        This is the low-level endpoint wrapper; most callers want
        :meth:`upload`, which drives the whole create → transfer → complete
        flow from a local file.

        Args:
            filename: Name the file will be stored under.
            file_size: Size in bytes (max 1GB). When positive the server
                initiates a multipart upload session and returns per-part
                URLs; pass ``0`` to request a single PUT URL instead.

        Returns:
            ``{"id": "<org_uuid>/<file_uuid>", "upload": {...}}``. The
            ``upload`` dict has ``mode`` ``"single"`` (with ``url``) or
            ``"multipart"`` (with ``upload_session_id`` and ``parts``).
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/reference_data",
            json={"filename": filename, "file_size": file_size},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code in (200, 201):
            result: Dict[str, Any] = response.json()
            return result
        raise Exception(
            f"Failed to create reference data upload: "
            f"{response.status_code} - {response.text}"
        )

    def complete_upload(
        self, reference_data_id: str, filename: str, upload_session_id: str
    ) -> bool:
        """Finalise a multipart reference data upload.

        Only needed for multipart uploads; a single PUT needs no completion.
        :meth:`upload` calls this for you when required.

        Args:
            reference_data_id: The ``id`` (``"<org_uuid>/<file_uuid>"``)
                returned by :meth:`create_upload`.
            filename: The uploaded filename.
            upload_session_id: The ``upload_session_id`` from the create
                response's ``upload`` dict.
        """
        response = self._client._make_request(
            method="POST",
            endpoint=f"/reference_data/{reference_data_id}/complete",
            json={"filename": filename, "upload_session_id": upload_session_id},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return True
        raise Exception(
            f"Failed to complete reference data upload: "
            f"{response.status_code} - {response.text}"
        )

    def list(self) -> List[Dict[str, str]]:
        """List reference data files for the current organisation.

        Returns:
            A list of ``{"id": "<org_uuid>/<file_uuid>", "filename": ...}``
            dicts.
        """
        response = self._client._make_request(
            method="GET",
            endpoint="/reference_data",
        )

        if response.status_code == 200:
            result: List[Dict[str, str]] = response.json()
            return result
        raise Exception(
            f"Failed to list reference data: "
            f"{response.status_code} - {response.text}"
        )

    def upload(self, file_path: str, filename: Optional[str] = None) -> str:
        """Upload a local file as organisation-scoped reference data.

        Drives the full flow: request a presigned URL, transfer the bytes
        (a single PUT for small files, multipart for large ones) and finalise
        the multipart upload when one was used.

        Args:
            file_path: Path to the local file to upload.
            filename: Name to store the file under. Defaults to the basename
                of ``file_path``.

        Returns:
            The reference data ``id`` (``"<org_uuid>/<file_uuid>"``); match it
            against the entries returned by :meth:`list`.
        """
        self._uploader._validate_file_exists(file_path)
        if filename is None:
            filename = os.path.basename(file_path)

        file_size = self._uploader._get_file_size(file_path)
        use_multipart = self._uploader.should_use_multipart(file_size)

        # file_size is required server-side; a positive value requests a
        # multipart session, 0 requests a single PUT URL.
        result = self.create_upload(filename, file_size if use_multipart else 0)
        reference_data_id = str(result["id"])
        upload = result["upload"]

        if upload.get("mode") == "multipart":
            self._uploader.upload_multipart_file(upload["parts"], file_path, filename)
            self.complete_upload(
                reference_data_id, filename, upload["upload_session_id"]
            )
        else:
            self._uploader.upload_single_file(upload["url"], file_path, filename)

        return reference_data_id
