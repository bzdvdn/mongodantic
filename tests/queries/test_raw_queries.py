import unittest
from bson import ObjectId
from uuid import uuid4, UUID

from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params
from mongodantic.session import Session


class TestBasicOperation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class User(MongoModel):
            id: UUID
            name: str
            email: str

            class Config:
                excluded_query_fields = ('sign', 'type')

        User.querybuilder.drop_collection(force=True)
        self.User = User

    def test_raw_insert_one(self):
        result = self.User.querybuilder.raw_query(
            'insert_one', {'id': uuid4(), 'name': 'first', 'email': 'first@mail.ru'}
        )
        assert isinstance(result.inserted_id, ObjectId)
