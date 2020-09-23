import pymongo
import unittest
import pytest
from pymongo import IndexModel
from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params
from mongodantic.exceptions import MongoIndexError


class TestIndexOperation(unittest.TestCase):
    def setUp(self, drop=True):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Ticket(MongoModel):
            name: str
            position: int
            config: dict

            class Config:
                indexes = [IndexModel([('position', 1)])]

        if drop:
            Ticket.querybuilder.drop_collection(force=True)
        self.Ticket = Ticket

    def test_check_indexes(self):
        self.setUp(False)
        result = self.Ticket.querybuilder.check_indexes()
        assert result == {
            '_id_': {'key': {'_id': 1}},
            'position_1': {'key': {'position': 1}},
        }

    def test_drop_index(self):
        self.setUp(False)
        with pytest.raises(MongoIndexError):
            result = self.Ticket.querybuilder.drop_index('position1111')

        result = self.Ticket.querybuilder.drop_index('position_1')
        assert result == 'position_1 dropped.'
