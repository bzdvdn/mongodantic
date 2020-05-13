import unittest
import pytest
from mongodantic.logical import Query, LogicalCombination
from mongodantic.models import MongoModel


class TestLogicalQuery(unittest.TestCase):
    def setUp(self):
        class Ticket(MongoModel):
            name: str
            position: int

        Ticket.drop_collection(force=True)
        self.Ticket = Ticket

    def test_query_organization(self):
        query = (
            Query(name=123)
            | Query(name__ne=124) & Query(position=1)
            | Query(position=2)
        )
        data = query.to_query(self.Ticket)
        value = {
            '$or': [
                {'name': '123'},
                {'$and': [{'name': {'$ne': 124}}, {'position': 1}]},
                {'position': 2},
            ]
        }
        assert data == value

    def test_logical_query_result(self):
        query = [
            self.Ticket(name='first', position=1),
            self.Ticket(name='second', position=2),
        ]
        inserted = self.Ticket.insert_many(query)
        assert inserted == 2

        query = Query(name='first') | Query(position=1) & Query(name='second')
        data = self.Ticket.find_one(query)
        assert data.name == 'first'

        query = Query(position=3) | Query(position=1) & Query(name='second')
        data = self.Ticket.find_one(query)
        assert data is None

        query = Query(position=3) | Query(position=2) & Query(name='second')
        data = self.Ticket.find_one(query)
        assert data.name == 'second'
