from unittest.mock import Mock
from uuid import UUID

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import Project
from md_python.resources.v2.projects import Projects

PROJECT_ID = "11111111-1111-1111-1111-111111111111"

PROJECT_JSON = {
    "id": PROJECT_ID,
    "name": "Plasma study",
    "description": "Pilot cohort",
    "tags": ["pilot", "plasma"],
}


class TestV2Projects:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def projects(self, mock_client):
        return Projects(mock_client)

    def test_list(self, projects, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [PROJECT_JSON]
        mock_client._make_request.return_value = mock_response

        result = projects.list()

        mock_client._make_request.assert_called_once_with(
            method="GET", endpoint="/projects"
        )
        assert len(result) == 1
        project = result[0]
        assert isinstance(project, Project)
        assert project.id == UUID(PROJECT_ID)
        assert project.name == "Plasma study"
        assert project.description == "Pilot cohort"
        assert project.tags == ["pilot", "plasma"]

    def test_list_empty(self, projects, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_client._make_request.return_value = mock_response

        assert projects.list() == []

    def test_list_failure(self, projects, mock_client):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to list projects: 401"):
            projects.list()

    def test_from_json_without_description_or_tags(self):
        project = Project.from_json(
            {"id": PROJECT_ID, "name": "Bare", "description": None, "tags": []}
        )
        assert project.description is None
        assert project.tags == []
        assert str(project).startswith("Project: Bare")

    def test_client_exposes_projects(self):
        client = MDClientV2(api_token="t", base_url="http://x")
        assert isinstance(client.projects, Projects)
