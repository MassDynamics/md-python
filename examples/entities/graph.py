"""
Example of mapping protein groups to protein groups using the MD Python client
"""

import os

from md_python import MDClientV2


def protein_to_protein_example():
    """Map protein groups to protein groups through their shared individual proteins."""

    client = MDClientV2(api_token=os.getenv("API_TOKEN"))

    result = client.entities.graph(
        mode='protein_to_metabolite',
        dataset_ids=[
            "c7215a65-dd68-4051-a158-f369b6eeed6a",
            "82d4d3ba-3733-4987-91d8-7f83d2afba09",
            "7e880efc-349d-42c6-869e-671af57bb970",
            "643d23ec-65a2-44c4-ada4-694032183945",
            "8b398f3e-6f3f-4a37-a5ea-bfe0a2edb4d6",
            "de83ea45-45fc-4242-9455-64b8236a6aac",
            "d7d5582f-7b15-405a-bed2-9d81f1a32ff6",
            "d2a485ca-df13-4974-8c05-c5adcdf7669c",
            "48ee0eeb-3d2e-422f-bdeb-41938ffad5b4",
            "5a6ce013-d77a-4eb4-8eb6-715f044ef2bc",
            "8a02d25c-68e5-4def-b090-fc4fe39f011d",
            "def259ea-9caa-4416-b2bd-750983138041"
        ],
        entity_ids=[
            "P19367",
        ],
    )

    print(f"Got {len(result['nodes'])} nodes and {len(result['edges'])} edges")

    for node in result["nodes"][:5]:
        print(f"  node: {node.get('~id')} ({node.get('~labels')})")


if __name__ == "__main__":
    protein_to_protein_example()
