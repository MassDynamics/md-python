"""
Example of mapping protein groups to protein groups using the MD Python client
"""

import os

from md_python import MDClientV2


def protein_to_protein_example():
    """Map protein groups to protein groups through their shared individual proteins."""

    client = MDClientV2(api_token=os.getenv("API_TOKEN"))

    result = client.entities.mappings.protein_to_protein(
        dataset_ids=[
            "cb6fcbaa-6b0b-49ec-ac59-696ca6ff4366",
            "adc1942c-e513-4adc-ab9b-d45483308883",
            "48ee0eeb-3d2e-422f-bdeb-41938ffad5b4",
            "5a6ce013-d77a-4eb4-8eb6-715f044ef2bc",
            "8a02d25c-68e5-4def-b090-fc4fe39f011d",
            "def259ea-9caa-4416-b2bd-750983138041",
        ],
        entity_ids=[
            "A0A3B3ISV8;Q8IZF0",
            "Q14061;H7C4E5;C9J8T6",
            "Q14696;H0YLI4;Q14696",
            "Q9NX09",
            "Q9UBK9;Q9UBK9;S4R2Z4",
        ],
    )

    print(f"Got {len(result['nodes'])} nodes and {len(result['edges'])} edges")

    for node in result["nodes"][:5]:
        print(f"  node: {node.get('~id')} ({node.get('~labels')})")


if __name__ == "__main__":
    protein_to_protein_example()
