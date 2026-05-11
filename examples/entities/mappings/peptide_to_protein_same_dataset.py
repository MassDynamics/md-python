"""
Example of mapping peptides to their protein groups within a single dataset
"""

import os

from md_python import MDClientV2


def peptide_to_protein_same_dataset_example():
    """Map peptides to their protein groups within a single dataset."""

    client = MDClientV2(api_token=os.getenv("API_TOKEN"))

    result = client.entities.mappings.peptide_to_protein_same_dataset(
        dataset_id="37fd98a9-f083-47ed-96db-7c2ccea9f45d",
        entity_ids=["AAS(UniMod:21)PEK", "VDGSNLEGGSQQGPST(UniMod:21)PPNTPDPR"],
    )

    print(f"Got {len(result['nodes'])} nodes and {len(result['edges'])} edges")

    for node in result["nodes"][:5]:
        print(f"  node: {node.get('~id')} ({node.get('~labels')})")


if __name__ == "__main__":
    peptide_to_protein_same_dataset_example()
