import unittest
import pytest
from bson import ObjectId
from uuid import uuid4, UUID

from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params
from mongodantic.session import Session
from mongodantic.exceptions import ValidationError


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
        with pytest.raises(ValidationError):
            result = self.User.querybuilder.raw_query(
                'insert_one', {'id': uuid4(), 'name': {}, 'email': []}
            )
        result = self.User.querybuilder.raw_query(
            'insert_one', {'id': uuid4(), 'name': 'first', 'email': 'first@mail.ru'}
        )
        assert isinstance(result.inserted_id, ObjectId)

    def test_raw_insert_many(self):
        with pytest.raises(ValidationError):
            result = self.User.querybuilder.raw_query(
                'insert_many', [{'id': uuid4(), 'name': {}, 'email': []}]
            )
        result = self.User.querybuilder.raw_query(
            'insert_many', [{'id': uuid4(), 'name': 'first', 'email': 'first@mail.ru'}]
        )
        assert len(result.inserted_ids) == 1

    def test_raw_find_one(self):
        self.test_raw_insert_one()
        result = self.User.querybuilder.raw_query('find_one', {'name': 'first'})
        assert result['name'] == 'first'
        assert result['email'] == 'first@mail.ru'

    def test_raw_update_one(self):
        self.test_raw_insert_one()
        with pytest.raises(ValidationError):
            result = self.User.querybuilder.raw_query(
                'update_one', [{'id': uuid4(), 'name': {}, 'email': []}]
            )
