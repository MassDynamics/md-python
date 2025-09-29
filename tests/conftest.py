import pytest


@pytest.fixture(autouse=True)
def _mock_env_for_client(monkeypatch):
    """Ensure tests don't depend on local .env values.

    - Force base URL to the value expected by tests
    - Clear auth token so explicit tokens in tests are used
    """
    monkeypatch.setenv("MD_API_BASE_URL", "https://app.massdynamics.com/api")
    monkeypatch.delenv("MD_AUTH_TOKEN", raising=False)

