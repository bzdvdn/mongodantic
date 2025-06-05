import unittest
import pytest
from bson import ObjectId
from uuid import uuid4, UUID

from mongodantic.models import MongoModel
from mongodantic import connect
from mongodantic.exceptions import MongoValidationError
from pydantic import ConfigDict
from mongodantic.querybuilder import QueryBuilder


class TestBasicOperation:
    def setup_method(self):
        connect("mongodb://127.0.0.1:27017", "test")

        class User(MongoModel):
            id: str
            name: str
            email: str
            model_config = ConfigDict(excluded_query_fields=('sign', 'type'))

        # Initialize the model with valid values to ensure Q is initialized
        # This ensures Q is initialized
        _ = User(id="test", name="test", email="test@test.com")
        self.User = User  # Store the class, not the instance
        # Ensure Q is initialized
        if not self.User.__querybuilder__:
            self.User.__querybuilder__ = QueryBuilder(self.User)
        # Now that Q is initialized, we can use it
        self.User.Q.drop_collection(force=True)

    def test_raw_insert_one(self):
        # Test with invalid data types that should trigger validation error
        with pytest.raises(MongoValidationError):
            result = self.User.Q.raw_query(
                'insert_one', {'id': 123, 'name': {},
                               'email': []}  # Using invalid types
            )

        # Test with valid data
        result = self.User.Q.raw_query(
            'insert_one',
            {'id': str(uuid4()), 'name': 'first', 'email': 'first@mail.ru'},
        )
        assert isinstance(result.inserted_id, ObjectId)

    # @pytest.mark.asyncio
    # async def test_async_raw_insert_one(self):
    #     with pytest.raises(MongoValidationError):
    #         result = await self.User.AQ.raw_query(
    #             'insert_one', {'id': uuid4(), 'name': {}, 'email': []}
    #         )
    #     result = await self.User.AQ.raw_query(
    #         'insert_one', {'id': uuid4(), 'name': 'first', 'email': 'first@mail.ru'}
    #     )
    #     assert isinstance(result.inserted_id, ObjectId)

    # @pytest.mark.asyncio
    # async def test_async_raw_insert_many(self):
    #     with pytest.raises(MongoValidationError):
    #         result = await self.User.AQ.raw_query(
    #             'insert_many', [{'id': uuid4(), 'name': {}, 'email': []}]
    #         )
    #     result = await self.User.AQ.raw_query(
    #         'insert_many', [{'id': uuid4(), 'name': 'first', 'email': 'first@mail.ru'}]
    #     )
    #     assert len(result.inserted_ids) == 1

    def test_raw_find_one(self):
        self.test_raw_insert_one()
        result = self.User.Q.raw_query('find_one', {'name': 'first'})
        assert result['name'] == 'first'
        assert result['email'] == 'first@mail.ru'

    # @pytest.mark.asyncio
    # async def test_async_raw_find_one(self):
    #     await self.test_async_raw_insert_one()
    #     result = await self.User.AQ.raw_query('find_one', {'name': 'first'})
    #     assert result['name'] == 'first'
    #     assert result['email'] == 'first@mail.ru'

    def test_raw_update_one(self):
        self.test_raw_insert_one()
        with pytest.raises(MongoValidationError):
            result = self.User.Q.raw_query(
                # Using invalid types
                'update_one', [{'id': 123, 'name': {}, 'email': []}]
            )
        result = self.User.Q.raw_query(
            'update_one', raw_query=({'name': 'first'}, {'$set': {'name': 'updated'}})
        )

        assert result.modified_count == 1

        modifed_result = self.User.Q.find_one(email='first@mail.ru')
        assert modifed_result.name == 'updated'

    # @pytest.mark.asyncio
    # async def test_async_raw_update_one(self):
    #     await self.test_async_raw_insert_one()
    #     with pytest.raises(MongoValidationError):
    #         result = await self.User.AQ.raw_query(
    #             'update_one', [{'id': uuid4(), 'name': {}, 'email': []}]
    #         )
    #     result = await self.User.AQ.raw_query(
    #         'update_one', raw_query=({'name': 'first'}, {'$set': {'name': 'updated'}})
    #     )

    #     assert result.modified_count == 1

    #     modifed_result = await self.User.AQ.find_one(email='first@mail.ru')
    #     assert modifed_result.name == 'updated'
