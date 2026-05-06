"""
Example of mapping protein groups to their peptides within a single dataset
"""

import os

from md_python import MDClientV2


def protein_to_peptide_same_dataset_example():
    """Map protein groups to their peptides within a single dataset."""

    client = MDClientV2(api_token=os.getenv("API_TOKEN"))

    result = client.entities.mappings.protein_to_peptide_same_dataset(
        dataset_id="21fd98a9-f083-47ed-12db-7c2ccea9f45d",
        entity_ids=[
            "P47757",
            "Q14696;H0YLI4;Q14696",
            "Q9NX09",
        ],
    )

    print(f"Got {len(result['nodes'])} nodes and {len(result['edges'])} edges")

    for node in result["nodes"][:5]:
        print(f"  node: {node.get('~id')} ({node.get('~labels')})")


if __name__ == "__main__":
    protein_to_peptide_same_dataset_example()
