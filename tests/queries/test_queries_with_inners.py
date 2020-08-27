import unittest
from bson import ObjectId

from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params
from mongodantic.session import Session


class TestQueriesWithInners(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class InnerTicket(MongoModel):
            name: str
            position: int
            config: dict
            sign: int = 1
            type_: str = 'ga'

            class Config:
                excluded_query_fields = ('sign', 'type')

        InnerTicket.querybuilder.drop_collection(force=True)
        self.InnerTicket = InnerTicket

    def create_documents(self):
        self.InnerTicket.querybuilder.insert_one(
            name='first', position=1, config={'url': 'localhost', 'username': 'admin'},
        )
        self.InnerTicket.querybuilder.insert_one(
            name='second',
            position=2,
            config={'url': 'google.com', 'username': 'staff'},
        )

    def test_inner_find_one(self):
        self.create_documents()
        data = self.InnerTicket.querybuilder.find_one(config__url__startswith='goo')
        assert data.name == 'second'

    def test_inner_update_one(self):
        self.create_documents()
        updated = self.InnerTicket.querybuilder.update_one(
            config__url__startswith='goo', config__url__set='test.io'
        )
        assert updated == 1
        data = self.InnerTicket.querybuilder.find_one(config__url__startswith='test')
        assert data.name == 'second'
