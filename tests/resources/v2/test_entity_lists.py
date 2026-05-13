"""Tests for the v2 entity_lists resource."""

from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import EntityList, EntityListItem
from md_python.resources.v2.entity_lists import EntityLists

WS_ID = "11111111-1111-1111-1111-111111111111"
LIST_ID = "22222222-2222-2222-2222-222222222222"
DATASET_ID = "33333333-3333-3333-3333-333333333333"


def _list_payload(**overrides):
    base = {
        "id": LIST_ID,
        "name": "Top hits",
        "type": "protein",
        "experiment_id": "44444444-4444-4444-4444-444444444444",
        "items_count": 2,
        "owner": True,
        "created_at": "2026-05-08T00:00:00Z",
        "updated_at": "2026-05-08T00:00:00Z",
        "items": [
            {
                "id": "55555555-5555-5555-5555-555555555555",
                "entity_id": "P12345",
                "group_id": 1,
                "dataset_external_id": DATASET_ID,
            },
            {
                "id": "66666666-6666-6666-6666-666666666666",
                "entity_id": "Q9999",
                "group_id": 2,
                "dataset_id": DATASET_ID,  # accept dataset_id too
            },
        ],
    }
    base.update(overrides)
    return base


def _response(status_code, json_body=None, text=""):
    response = Mock()
    response.status_code = status_code
    if json_body is not None:
        response.json.return_value = json_body
    response.text = text
    return response


class TestCreate:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def lists(self, mock_client):
        return EntityLists(mock_client)

    def test_create_with_dict_items(self, lists, mock_client):
        mock_client._make_request.return_value = _response(201, _list_payload())
        result = lists.create(
            workspace_id=WS_ID,
            name="Top hits",
            entity_type="protein",
            items=[
                {"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID},
                {"entity_id": "Q9999", "group_id": 2, "dataset_id": DATASET_ID},
            ],
        )

        call = mock_client._make_request.call_args
        assert call[1]["method"] == "POST"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/entity_lists"
        assert call[1]["json"] == {
            "name": "Top hits",
            "entity_type": "protein",
            "items": [
                {"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID},
                {"entity_id": "Q9999", "group_id": 2, "dataset_id": DATASET_ID},
            ],
        }
        assert isinstance(result, EntityList)
        assert result.name == "Top hits"
        assert result.type == "protein"
        assert result.items_count == 2
        assert len(result.items) == 2
        assert result.items[0].entity_id == "P12345"

    def test_create_with_model_items(self, lists, mock_client):
        mock_client._make_request.return_value = _response(201, _list_payload())
        lists.create(
            workspace_id=WS_ID,
            name="Top hits",
            entity_type="protein",
            items=[
                EntityListItem(entity_id="P12345", group_id=1, dataset_id=DATASET_ID)
            ],
        )
        payload = mock_client._make_request.call_args[1]["json"]
        assert payload["items"] == [
            {"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID}
        ]

    def test_rejects_invalid_entity_type(self, lists):
        with pytest.raises(ValueError, match="entity_type must be one of"):
            lists.create(
                workspace_id=WS_ID,
                name="x",
                entity_type="metabolite",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )

    def test_rejects_empty_items(self, lists):
        with pytest.raises(ValueError, match="at least one"):
            lists.create(workspace_id=WS_ID, name="x", entity_type="protein", items=[])

    def test_rejects_item_without_entity_id(self, lists, mock_client):
        with pytest.raises(ValueError, match="entity_id"):
            lists.create(
                workspace_id=WS_ID,
                name="x",
                entity_type="protein",
                items=[{"group_id": 1, "dataset_id": DATASET_ID}],
            )
        mock_client._make_request.assert_not_called()

    def test_rejects_non_dict_non_model_item(self, lists):
        with pytest.raises(TypeError, match="EntityListItem or dict"):
            lists.create(
                workspace_id=WS_ID,
                name="x",
                entity_type="protein",
                items=["P12345"],
            )

    def test_server_error_propagates(self, lists, mock_client):
        mock_client._make_request.return_value = _response(400, text="bad name")
        with pytest.raises(Exception, match="400"):
            lists.create(
                workspace_id=WS_ID,
                name="x",
                entity_type="protein",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )


class TestGet:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def lists(self, mock_client):
        return EntityLists(mock_client)

    def test_returns_entity_list_on_200(self, lists, mock_client):
        mock_client._make_request.return_value = _response(200, _list_payload())
        result = lists.get(WS_ID, LIST_ID)
        assert result is not None
        assert result.name == "Top hits"
        # Server can return items with dataset_external_id (persisted shape)
        # or dataset_id — both normalise to dataset_id on the model.
        assert all(item.dataset_id == DATASET_ID for item in result.items)
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "GET"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/entity_lists/{LIST_ID}"

    def test_returns_none_on_404(self, lists, mock_client):
        mock_client._make_request.return_value = _response(404)
        assert lists.get(WS_ID, LIST_ID) is None


class TestUpdate:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def lists(self, mock_client):
        return EntityLists(mock_client)

    def test_update_replaces_all_fields(self, lists, mock_client):
        mock_client._make_request.return_value = _response(200, _list_payload())
        result = lists.update(
            workspace_id=WS_ID,
            list_id=LIST_ID,
            name="Top hits",
            entity_type="protein",
            items=[
                {"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID},
            ],
        )
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "PUT"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/entity_lists/{LIST_ID}"
        assert call[1]["json"] == {
            "name": "Top hits",
            "entity_type": "protein",
            "items": [{"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID}],
        }
        assert isinstance(result, EntityList)

    def test_update_accepts_model_items(self, lists, mock_client):
        mock_client._make_request.return_value = _response(200, _list_payload())
        lists.update(
            workspace_id=WS_ID,
            list_id=LIST_ID,
            name="x",
            entity_type="protein",
            items=[
                EntityListItem(entity_id="P12345", group_id=1, dataset_id=DATASET_ID)
            ],
        )
        payload = mock_client._make_request.call_args[1]["json"]
        assert payload["items"] == [
            {"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID}
        ]

    def test_update_rejects_invalid_entity_type(self, lists):
        with pytest.raises(ValueError, match="entity_type must be one of"):
            lists.update(
                workspace_id=WS_ID,
                list_id=LIST_ID,
                name="x",
                entity_type="metabolite",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )

    def test_update_rejects_empty_items(self, lists):
        with pytest.raises(ValueError, match="at least one"):
            lists.update(
                workspace_id=WS_ID,
                list_id=LIST_ID,
                name="x",
                entity_type="protein",
                items=[],
            )

    def test_update_propagates_server_error(self, lists, mock_client):
        mock_client._make_request.return_value = _response(400, text="bad payload")
        with pytest.raises(Exception, match="400"):
            lists.update(
                workspace_id=WS_ID,
                list_id=LIST_ID,
                name="x",
                entity_type="protein",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )


class TestDelete:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def lists(self, mock_client):
        return EntityLists(mock_client)

    def test_delete_returns_none_on_204(self, lists, mock_client):
        mock_client._make_request.return_value = _response(204)
        result = lists.delete(WS_ID, LIST_ID)
        assert result is None
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "DELETE"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/entity_lists/{LIST_ID}"

    def test_delete_propagates_server_error(self, lists, mock_client):
        mock_client._make_request.return_value = _response(404, text="missing")
        with pytest.raises(Exception, match="404"):
            lists.delete(WS_ID, LIST_ID)
