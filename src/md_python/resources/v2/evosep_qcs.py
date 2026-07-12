"""
Evosep QCs sub-resource for the MD Python v2 client
"""

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


class EvosepQcs:
    """V2 Evosep QCs sub-resource.

    Wraps ``POST /evosep_qcs`` (workflow app/api/api/v2/evosep_qcs/create.rb).
    The endpoint is feature-flagged behind the Flipper flag ``evosep_qc`` — when
    the flag is off for the caller the server returns 404 ``{"error": "Not
    found"}``. That is an expected "feature not enabled for this account" state,
    so the 404 body is surfaced verbatim in the raised exception rather than
    masked.
    """

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def create(self, filename: str, blob: Dict[str, Any]) -> Dict[str, Any]:
        """Create an Evosep QC record.

        Args:
            filename: Name of the uploaded file.
            blob: Arbitrary JSON contents of the file.

        Returns:
            The created record as a dict: {id, filename, uploaded_by, created_at}.

        Raises:
            Exception: On any non-201 response, including the feature-flag-off
                404 ({"error": "Not found"}) and 422 validation failures
                ({"errors": [...]}). The status code and response body are
                included in the message.
        """
        response = self._client._make_request(
            method="POST",
            endpoint="/evosep_qcs",
            json={"filename": filename, "blob": blob},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 201:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to create evosep_qc: {response.status_code} - {response.text}"
            )
