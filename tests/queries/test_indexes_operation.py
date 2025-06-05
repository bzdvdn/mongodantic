import pymongo
import unittest
import pytest
from pymongo import IndexModel
from mongodantic.models import MongoModel
from mongodantic import connect, init_db_connection_params
from mongodantic.exceptions import MongoIndexError
from mongodantic.connection import _connections
from mongodantic.querybuilder import QueryBuilder, AsyncQueryBuilder
from typing import Dict, Any


class TestIndexOperation:
    def setup_method(self):
        """Set up test environment before each test method."""
        # Define the Ticket model with indexes
        class Ticket(MongoModel):
            name: str
            position: int
            config: Dict[str, Any] = {}

            model_config = {
                'indexes': [
                    IndexModel([('position', 1)], background=False),
                    IndexModel([('name', 1)], background=False)
                ]
            }

        self.Ticket = Ticket

        # Force initialization of query builders
        self.Ticket.__querybuilder__ = QueryBuilder(self.Ticket)
        self.Ticket.__async_querybuilder__ = AsyncQueryBuilder(self.Ticket)

        # Create indexes and verify they exist
        try:
            # Create indexes directly using the collection
            collection = self.Ticket._collection
            collection.create_index([('position', 1)], background=False)
            collection.create_index([('name', 1)], background=False)

            # Verify indexes were created
            indexes = self.Ticket.Q.check_indexes()
            print("Indexes after creation:", indexes)

            if not indexes:
                raise Exception("Failed to create indexes during setup")
        except Exception as e:
            print(f"Error creating indexes: {str(e)}")
            raise

    def teardown_method(self):
        """Clean up after each test method."""
        try:
            # Drop the collection
            if hasattr(self, 'Ticket') and self.Ticket.Q is not None:
                try:
                    self.Ticket.Q.drop_collection(force=True)
                except Exception as e:
                    print(f"Error dropping collection: {str(e)}")
        except Exception as e:
            print(f"Error in teardown: {str(e)}")

        # Don't close connections here - let the session fixture handle it
        # Just clear the local references
        _connections.clear()

    def test_check_indexes(self):
        """Test that indexes are created correctly."""
        result = self.Ticket.Q.check_indexes()
        print("Indexes in test_check_indexes:", result)
        assert result == {
            '_id_': {'key': {'_id': 1}},
            'position_1': {'key': {'position': 1}},
            'name_1': {'key': {'name': 1}},
        }

    def test_check_indexes_if_remove(self):
        """Test that indexes can be removed."""
        # Drop the name index
        self.Ticket.Q.drop_index('name_1')
        result = self.Ticket.Q.check_indexes()
        print("Indexes in test_check_indexes_if_remove:", result)
        assert result == {
            '_id_': {'key': {'_id': 1}},
            'position_1': {'key': {'position': 1}},
        }

    def test_drop_index(self):
        """Test that dropping a non-existent index raises an error."""
        with pytest.raises(MongoIndexError):
            self.Ticket.Q.drop_index('position1111')

        result = self.Ticket.Q.drop_index('position_1')
        assert result == 'position_1 dropped.'


# Initialize connection for Product model
init_db_connection_params("mongodb://127.0.0.1:27017", "test")


class Product(MongoModel):
    title: str
    cost: float
    quantity: int
    product_type: str
    config: dict

    model_config = {
        'indexes': [
            IndexModel([('title', 1)]),
            IndexModel([('cost', 1)]),
        ]
    }
