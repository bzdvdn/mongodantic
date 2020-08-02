import pymongo
import unittest
import pytest
from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params
from mongodantic.exceptions import MongoIndexError


class TestIndexOperation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Ticket(MongoModel):
            name: str
            position: int
            config: dict

        Ticket.querybuilder.drop_collection(force=True)
        self.Ticket = Ticket

    def test_add_index(self):
        result = self.Ticket.querybuilder.add_index('position', 1)
        assert result == 'index with name - position created.'

        with pytest.raises(MongoIndexError):
            result = self.Ticket.querybuilder.add_index('position', 1)

    def test_check_indexes(self):
        self.test_add_index()
        result = self.Ticket.querybuilder.check_indexes()
        assert result == [
            {'name': '_id_', 'key': {'_id': 1}},
            {'name': 'position_1', 'key': {'position': 1}},
        ]

    def test_drop_index(self):
        self.test_add_index()
        with pytest.raises(MongoIndexError):
            result = self.Ticket.querybuilder.drop_index('position1111', 1)

        result = self.Ticket.querybuilder.drop_index('position', 1)
        assert result == 'position dropped.'
