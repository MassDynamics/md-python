from uuid import UUID

import pytest

from md_python.client import MDClient
from md_python.models import Dataset
from md_python.resources.datasets import Datasets


def ds(state: str, id_str: str = "11111111-1111-1111-1111-111111111111") -> Dataset:
    return Dataset(
        input_dataset_ids=[],
        name="n",
        job_slug="j",
        job_run_params={},
        state=state,
        id=UUID(id_str),
    )


class TestDatasetsWait:
    @pytest.fixture
    def mock_client(self, mocker):
        return mocker.Mock(spec=MDClient)

    @pytest.fixture
    def res(self, mock_client):
        return Datasets(mock_client)

    def test_wait_until_complete_success(self, res, mocker):
        # return COMPLETED on first poll to avoid timeout flakiness
        mocker.patch.object(res, "list_by_experiment", return_value=[ds("COMPLETED")])
        out = res.wait_until_complete(
            "exp-1", "11111111-1111-1111-1111-111111111111", poll_s=0, timeout_s=1
        )
        assert isinstance(out, dict) or isinstance(out, Dataset)

    def test_wait_until_complete_failure(self, res, mocker):
        mocker.patch.object(res, "list_by_experiment", return_value=[ds("FAILED")])
        with pytest.raises(Exception):
            res.wait_until_complete(
                "exp-1", "11111111-1111-1111-1111-111111111111", poll_s=0, timeout_s=1
            )

    def test_find_initial_dataset(self, res, mock_client, mocker):
        # name preference via experiments.get_by_id
        mock_exp = mocker.Mock()
        mock_exp.name = "X"
        # ensure nested attribute exists on spec mock
        mock_client.experiments = mocker.Mock()
        mock_client.experiments.get_by_id.return_value = mock_exp
        d_int = ds("COMPLETED")
        d_int.type = "INTENSITY"
        d_int.name = "X"
        mocker.patch.object(res, "list_by_experiment", return_value=[d_int])
        out = res.find_initial_dataset("exp-1")
        assert out is d_int
