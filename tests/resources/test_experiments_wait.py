import pytest

from md_python.client import MDClient
from md_python.models import Experiment
from md_python.resources.experiments import Experiments


def make_exp(status: str) -> Experiment:
    return Experiment(name="x", source="s", s3_bucket="b", filenames=[], status=status)


class TestExperimentsWait:
    @pytest.fixture
    def mock_client(self, mocker):
        return mocker.Mock(spec=MDClient)

    @pytest.fixture
    def res(self, mock_client):
        return Experiments(mock_client)

    def test_wait_until_complete_success(self, res, mocker):
        mocker.patch.object(
            res,
            "get_by_id",
            side_effect=[make_exp("PROCESSING"), make_exp("COMPLETED")],
        )
        out = res.wait_until_complete("exp-1", poll_s=0, timeout_s=2)
        assert isinstance(out, Experiment)
        assert out.status == "COMPLETED"

    def test_wait_until_complete_failure(self, res, mocker):
        mocker.patch.object(res, "get_by_id", return_value=make_exp("FAILED"))
        with pytest.raises(Exception):
            res.wait_until_complete("exp-1", poll_s=0, timeout_s=1)
