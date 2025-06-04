from mongodantic import MongoModel
from mongodantic.querybuilder import Query


class MockTestModel(MongoModel):
    name: str
    value: int


def test_mock_connection():
    """Test that we can connect and perform basic operations with the mock database."""
    # Create a test document
    test_doc = MockTestModel(name="test", value=42)
    test_doc.save()

    # Verify we can retrieve it
    retrieved = MockTestModel.Q.find_one(Query(name="test"))
    assert retrieved is not None
    assert retrieved.name == "test"
    assert retrieved.value == 42

    # Verify we can update it
    test_doc.value = 43
    test_doc.save()

    updated = MockTestModel.Q.find_one(Query(_id=test_doc._id))
    assert updated.value == 43

    # Verify we can delete it
    test_doc.delete()
    deleted = MockTestModel.Q.find_one(Query(name="test"))
    assert deleted is None
