from uuid import UUID

from md_python.models import ReferenceDataFile

ORG = "11111111-1111-1111-1111-111111111111"
FILE = "22222222-2222-2222-2222-222222222222"
REF_ID = f"{ORG}/{FILE}"


def test_from_json():
    rd = ReferenceDataFile.from_json({"id": REF_ID, "filename": "genes.csv"})

    assert rd.id == REF_ID
    assert rd.filename == "genes.csv"


def test_from_json_missing_filename_defaults_empty():
    rd = ReferenceDataFile.from_json({"id": REF_ID})

    assert rd.filename == ""


def test_id_halves():
    rd = ReferenceDataFile(id=REF_ID, filename="genes.csv")

    assert rd.organisation_uuid == UUID(ORG)
    assert rd.file_uuid == UUID(FILE)


def test_equality():
    a = ReferenceDataFile(id=REF_ID, filename="genes.csv")
    b = ReferenceDataFile.from_json({"id": REF_ID, "filename": "genes.csv"})

    assert a == b
