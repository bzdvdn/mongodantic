import pytest
from mongodantic import MongoModel
from mongodantic.querybuilder import QueryBuilder, AsyncQueryBuilder
from typing import Dict, Any


class TestQueriesWithInners:
    def setup_method(self):
        """Set up test environment before each test method."""
        # Define the InnerTicket model
        class InnerTicket(MongoModel):
            name: str
            position: int
            config: Dict[str, Any] = {}
            params: Dict[str, Any] = {}

        self.InnerTicket = InnerTicket

        # Force initialization of query builders
        self.InnerTicket.__querybuilder__ = QueryBuilder(self.InnerTicket)
        self.InnerTicket.__async_querybuilder__ = AsyncQueryBuilder(
            self.InnerTicket)

        # Clean up any existing documents
        try:
            self.InnerTicket.Q.drop_collection(force=True)
        except Exception as e:
            print(f"Error cleaning collection: {str(e)}")

    def teardown_method(self):
        """Clean up after each test method."""
        try:
            self.InnerTicket.Q.drop_collection(force=True)
        except Exception as e:
            print(f"Error cleaning collection: {str(e)}")

    def create_documents(self):
        """Create test documents."""
        # Clean up any existing documents first
        self.InnerTicket.Q.drop_collection(force=True)

        # Create test documents
        self.InnerTicket.Q.insert_one(
            name='first',
            position=1,
            config={'url': 'localhost', 'username': 'admin'},
            params={},
        )
        self.InnerTicket.Q.insert_one(
            name='second',
            position=2,
            config={'url': 'localhost', 'username': 'user'},
            params={},
        )

        # Verify we have exactly 2 documents
        count = self.InnerTicket.Q.count_documents()
        assert count == 2, f"Expected 2 documents, got {count}"

    def test_update_many(self):
        """Test updating multiple documents with inner fields."""
        self.create_documents()

        # Update all documents with url='localhost'
        result = self.InnerTicket.Q.update_many(
            config__url='localhost',
            config__username__set='newuser'
        )
        assert result == 2  # The method returns the number of modified documents

        # Verify the update
        docs = list(self.InnerTicket.Q.find())
        assert len(docs) == 2
        assert all(doc.config['username'] == 'newuser' for doc in docs)

    def test_inner_find_one(self):
        """Test finding a document with inner fields."""
        self.create_documents()

        # Find document with specific inner field values
        doc = self.InnerTicket.Q.find_one(
            config__url='localhost',
            config__username='admin'
        )
        assert doc is not None
        assert doc.name == 'first'
        assert doc.config['username'] == 'admin'

    def test_inner_update_one(self):
        """Test updating a single document with inner fields."""
        self.create_documents()

        # Update first document
        result = self.InnerTicket.Q.update_one(
            config__url='localhost',
            config__username='admin',
            config__username__set='superadmin'
        )
        assert result == 1  # The method returns the number of modified documents

        # Verify the update
        doc = self.InnerTicket.Q.find_one(
            config__url='localhost',
            config__username='superadmin'
        )
        assert doc is not None
        assert doc.name == 'first'
        assert doc.config['username'] == 'superadmin'
