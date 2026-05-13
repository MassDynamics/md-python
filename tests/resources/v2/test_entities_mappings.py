from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.entities_mappings import EntitiesMappings


class TestV2EntitiesMappings:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def mappings(self, mock_client):
        return EntitiesMappings(mock_client)

    def test_protein_to_protein_success(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "nodes": [{"~id": "protein:P12345"}, {"~id": "protein_group:10"}],
            "edges": [{"~id": "e1"}],
        }
        mock_client._make_request.return_value = mock_response

        result = mappings.protein_to_protein(
            dataset_ids=["abc-123"], entity_ids=["P12345;Q67890"]
        )

        assert result == {
            "nodes": [{"~id": "protein:P12345"}, {"~id": "protein_group:10"}],
            "edges": [{"~id": "e1"}],
        }

        assert mock_client._make_request.call_args.kwargs == {
            "method": "POST",
            "endpoint": "/entities/mappings/protein_to_protein",
            "json": {"dataset_ids": ["abc-123"], "entity_ids": ["P12345;Q67890"]},
            "headers": {"Content-Type": "application/json"},
        }

    def test_protein_to_protein_failure(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "upstream boom"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to map protein_to_protein: 502"):
            mappings.protein_to_protein(dataset_ids=["abc-123"], entity_ids=["P12345"])

    def test_protein_to_peptide_same_dataset_success(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "nodes": [
                {"~id": "protein_group:10"},
                {"~id": "peptide:aas"},
            ],
            "edges": [{"~id": "e1"}],
        }
        mock_client._make_request.return_value = mock_response

        result = mappings.protein_to_peptide_same_dataset(
            dataset_id="abc-123", entity_ids=["P12345;Q67890"]
        )

        assert result == {
            "nodes": [
                {"~id": "protein_group:10"},
                {"~id": "peptide:aas"},
            ],
            "edges": [{"~id": "e1"}],
        }

        assert mock_client._make_request.call_args.kwargs == {
            "method": "POST",
            "endpoint": "/entities/mappings/protein_to_peptide/same_dataset",
            "json": {"dataset_id": "abc-123", "entity_ids": ["P12345;Q67890"]},
            "headers": {"Content-Type": "application/json"},
        }

    def test_protein_to_peptide_same_dataset_failure(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "upstream boom"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(
            Exception, match="Failed to map protein_to_peptide_same_dataset: 502"
        ):
            mappings.protein_to_peptide_same_dataset(
                dataset_id="abc-123", entity_ids=["P12345"]
            )

    def test_peptide_to_protein_same_dataset_success(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "nodes": [
                {"~id": "peptide:aas"},
                {"~id": "protein_group:10"},
            ],
            "edges": [{"~id": "e1"}],
        }
        mock_client._make_request.return_value = mock_response

        result = mappings.peptide_to_protein_same_dataset(
            dataset_id="abc-123", entity_ids=["AAS(UniMod:21)PEK"]
        )

        assert result == {
            "nodes": [
                {"~id": "peptide:aas"},
                {"~id": "protein_group:10"},
            ],
            "edges": [{"~id": "e1"}],
        }

        assert mock_client._make_request.call_args.kwargs == {
            "method": "POST",
            "endpoint": "/entities/mappings/peptide_to_protein/same_dataset",
            "json": {"dataset_id": "abc-123", "entity_ids": ["AAS(UniMod:21)PEK"]},
            "headers": {"Content-Type": "application/json"},
        }

    def test_peptide_to_protein_same_dataset_failure(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "upstream boom"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(
            Exception, match="Failed to map peptide_to_protein_same_dataset: 502"
        ):
            mappings.peptide_to_protein_same_dataset(
                dataset_id="abc-123", entity_ids=["AAS(UniMod:21)PEK"]
            )

    def test_gene_to_protein_success(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "nodes": [{"~id": "gene:ENSG1"}, {"~id": "protein_group:10"}],
            "edges": [{"~id": "e1"}],
        }
        mock_client._make_request.return_value = mock_response

        result = mappings.gene_to_protein(
            dataset_ids=["abc-123"], entity_ids=["ENSG00000139618"]
        )

        assert result == {
            "nodes": [{"~id": "gene:ENSG1"}, {"~id": "protein_group:10"}],
            "edges": [{"~id": "e1"}],
        }
        assert mock_client._make_request.call_args.kwargs == {
            "method": "POST",
            "endpoint": "/entities/mappings/gene_to_protein",
            "json": {
                "dataset_ids": ["abc-123"],
                "entity_ids": ["ENSG00000139618"],
            },
            "headers": {"Content-Type": "application/json"},
        }

    def test_gene_to_protein_failure(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "upstream boom"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to map gene_to_protein: 502"):
            mappings.gene_to_protein(dataset_ids=["abc"], entity_ids=["ENSG1"])

    def test_protein_to_gene_success(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "nodes": [{"~id": "protein_group:10"}, {"~id": "gene:ENSG1"}],
            "edges": [{"~id": "e1"}],
        }
        mock_client._make_request.return_value = mock_response

        result = mappings.protein_to_gene(
            dataset_ids=["abc-123"], entity_ids=["P12345"]
        )

        assert result == {
            "nodes": [{"~id": "protein_group:10"}, {"~id": "gene:ENSG1"}],
            "edges": [{"~id": "e1"}],
        }
        assert mock_client._make_request.call_args.kwargs == {
            "method": "POST",
            "endpoint": "/entities/mappings/protein_to_gene",
            "json": {"dataset_ids": ["abc-123"], "entity_ids": ["P12345"]},
            "headers": {"Content-Type": "application/json"},
        }

    def test_protein_to_gene_failure(self, mappings, mock_client):
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "upstream boom"
        mock_client._make_request.return_value = mock_response

        with pytest.raises(Exception, match="Failed to map protein_to_gene: 502"):
            mappings.protein_to_gene(dataset_ids=["abc"], entity_ids=["P12345"])
