import pytest

from md_python.base_client import BaseMDClient
from md_python.client import MDClient
from md_python.client_v1 import MDClientV1
from md_python.client_v2 import MDClientV2


class TestMDClientFactory:

    def test_default_returns_v1(self):
        client = MDClient(api_token="tok")
        assert isinstance(client, MDClientV1)

    def test_explicit_v1(self):
        client = MDClient(api_token="tok", version="v1")
        assert isinstance(client, MDClientV1)

    def test_explicit_v2(self):
        client = MDClient(api_token="tok", version="v2")
        assert isinstance(client, MDClientV2)

    def test_invalid_version_raises(self):
        with pytest.raises(ValueError, match="Unsupported API version"):
            MDClient(api_token="tok", version="v3")

    def test_both_are_base_client_subclasses(self):
        v1 = MDClient(api_token="tok", version="v1")
        v2 = MDClient(api_token="tok", version="v2")
        assert isinstance(v1, BaseMDClient)
        assert isinstance(v2, BaseMDClient)

    def test_custom_base_url_forwarded(self):
        client = MDClient(api_token="tok", base_url="https://custom.com/api", version="v2")
        assert client.base_url == "https://custom.com/api"


class TestMDClientV2:

    def test_accept_header(self):
        client = MDClientV2(api_token="tok")
        assert client.ACCEPT_HEADER == "application/vnd.md-v2+json"

    def test_headers_contain_v2_accept(self):
        client = MDClientV2(api_token="tok")
        headers = client._get_headers()
        assert headers["accept"] == "application/vnd.md-v2+json"
        assert headers["Authorization"] == "Bearer tok"

    def test_has_uploads_resource(self):
        client = MDClientV2(api_token="tok")
        assert hasattr(client, "uploads")

    def test_has_datasets_resource(self):
        client = MDClientV2(api_token="tok")
        assert hasattr(client, "datasets")

    def test_has_jobs_resource(self):
        client = MDClientV2(api_token="tok")
        assert hasattr(client, "jobs")

    def test_has_health_resource(self):
        client = MDClientV2(api_token="tok")
        assert hasattr(client, "health")

    def test_no_experiments_resource(self):
        client = MDClientV2(api_token="tok")
        assert not hasattr(client, "experiments")

    def test_missing_token_raises(self):
        with pytest.raises(ValueError, match="MD_AUTH_TOKEN"):
            MDClientV2()


class TestMDClientV1Resources:

    def test_has_experiments(self):
        client = MDClientV1(api_token="tok")
        assert hasattr(client, "experiments")

    def test_has_datasets(self):
        client = MDClientV1(api_token="tok")
        assert hasattr(client, "datasets")

    def test_has_health(self):
        client = MDClientV1(api_token="tok")
        assert hasattr(client, "health")

    def test_no_uploads_resource(self):
        client = MDClientV1(api_token="tok")
        assert not hasattr(client, "uploads")

    def test_no_jobs_resource(self):
        client = MDClientV1(api_token="tok")
        assert not hasattr(client, "jobs")

    def test_accept_header(self):
        client = MDClientV1(api_token="tok")
        assert client.ACCEPT_HEADER == "application/vnd.md-v1+json"
