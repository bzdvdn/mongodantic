import re
import pytest
from typing import List, Dict, Any
from bson import ObjectId
from pymongo.errors import ConnectionFailure
from pydantic import ValidationError
from pydantic_core import PydanticCustomError

from mongodantic.helpers import (
    ExtraQueryMapper,
    handle_and_convert_connection_errors,
    chunk_by_length,
    bulk_query_generator,
    cached_classproperty,
    group_by_aggregate_generation,
    classproperty,
    sort_validation,
    generate_name_field,
    _validate_value,
)
from mongodantic.models import MongoModel
from mongodantic import connect
from mongodantic.exceptions import MongoConnectionError, MongoValidationError


class TestExtraQueryMapper:
    def setup_method(self):
        connect("mongodb://127.0.0.1:27017", "test")

        class User(MongoModel):
            name: str
            date: str
            counter: int

        self.User = User

    def test_in_extra_param(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'name').extra_query(['in'], (1, 3))

    def test_nin_extra_param(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'name').extra_query(['nin'], (1, 3))

    def test_ne_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['ne'], 'test')
        assert extra == {'name': {'$ne': 'test'}}

    def test_regex_extra_param(self):
        extra = ExtraQueryMapper(
            self.User, 'name').extra_query(['regex'], 'test')
        assert extra == {'name': {'$regex': 'test'}}

    def test_regex_ne_extra_param(self):
        extra = ExtraQueryMapper(
            self.User, 'name').extra_query(['regex_ne'], 'test')
        assert isinstance(extra['name']['$not'], re.Pattern)
        assert extra['name']['$not'].pattern == 'test'

    def test_startswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(
            ['startswith'], 'test')
        assert extra == {'name': {'$regex': '^test'}}

    def test_endswith_extra_param(self):
        extra = ExtraQueryMapper(
            self.User, 'name').extra_query(['endswith'], 'test')
        assert extra == {'name': {'$regex': 'test$'}}

    def test_not_endswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(
            ['not_endswith'], 'test'
        )
        assert isinstance(extra['name']['$not'], re.Pattern)
        assert extra['name']['$not'].pattern == 'test$'

    def test_not_startswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(
            ['not_startswith'], 'test'
        )
        assert isinstance(extra['name']['$not'], re.Pattern)
        assert extra['name']['$not'].pattern == '^test'

    def test_range_extra_param(self):
        with pytest.raises(ValueError):
            ExtraQueryMapper(self.User, 'date').extra_query(['range'], 'test')

    def test_lts_gts_params(self):
        extra = ExtraQueryMapper(
            self.User, 'date').extra_query(['lt'], '2020-01-01')
        assert extra == {'date': {'$lt': '2020-01-01'}}

        extra = ExtraQueryMapper(self.User, 'date').extra_query(
            ['lte'], '2020-01-01')
        assert extra == {'date': {'$lte': '2020-01-01'}}

        extra = ExtraQueryMapper(
            self.User, 'date').extra_query(['gt'], '2020-01-01')
        assert extra == {'date': {'$gt': '2020-01-01'}}

        extra = ExtraQueryMapper(self.User, 'date').extra_query(
            ['gte'], '2020-01-01')
        assert extra == {'date': {'$gte': '2020-01-01'}}

    def test_inc_params(self):
        with pytest.raises(ValueError):
            ExtraQueryMapper(self.User, 'counter').extra_query(
                ['inc'], '2313123131')

    def test_exists_params(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'counter').extra_query(
                ['exists'], '2313123131')


class TestChunkByLength:
    def test_chunking_list(self):
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunks = list(chunk_by_length(data, 3))
        assert len(chunks) == 4
        assert chunks[0] == [1, 2, 3]
        assert chunks[1] == [4, 5, 6]
        assert chunks[2] == [7, 8, 9]
        assert chunks[3] == [10]

    def test_empty_list(self):
        data = []
        chunks = list(chunk_by_length(data, 3))
        assert len(chunks) == 0

    def test_chunk_size_larger_than_data(self):
        data = [1, 2, 3]
        chunks = list(chunk_by_length(data, 5))
        assert len(chunks) == 1
        assert chunks[0] == [1, 2, 3]


class TestBulkQueryGenerator:
    def setup_method(self):
        class TestModel(MongoModel):
            name: str
            value: int

        self.TestModel = TestModel
        self.test_objects = [
            TestModel(name="test1", value=1, _id=ObjectId()),
            TestModel(name="test2", value=2, _id=ObjectId()),
        ]

    def test_update_fields(self):
        queries = bulk_query_generator(
            self.test_objects,
            updated_fields=['name', 'value']
        )
        assert len(queries) == 2
        assert queries[0]._doc['$set'] == {'name': 'test1', 'value': 1}
        assert queries[1]._doc['$set'] == {'name': 'test2', 'value': 2}

    def test_query_fields(self):
        queries = bulk_query_generator(
            self.test_objects,
            query_fields=['name']
        )
        assert len(queries) == 2
        # Check that the query contains the name field
        assert queries[0]._doc['$set']['name'] == 'test1'
        assert queries[1]._doc['$set']['name'] == 'test2'
        # Check that the query contains the value field
        assert queries[0]._doc['$set']['value'] == 1
        assert queries[1]._doc['$set']['value'] == 2

    def test_upsert(self):
        queries = bulk_query_generator(
            self.test_objects,
            query_fields=['name'],
            upsert=True
        )
        assert len(queries) == 2
        # Check that the query contains the name and value fields
        assert queries[0]._doc['$set']['name'] == 'test1'
        assert queries[0]._doc['$set']['value'] == 1
        assert queries[1]._doc['$set']['name'] == 'test2'
        assert queries[1]._doc['$set']['value'] == 2


class TestCachedClassProperty:
    def test_caching(self):
        class TestClass:
            counter = 0

            @cached_classproperty
            def test_property(cls):
                cls.counter += 1
                return cls.counter

        assert TestClass.test_property == 1
        assert TestClass.test_property == 1  # Should use cached value
        assert TestClass.counter == 1  # Counter should not increment


class TestGroupByAggregateGeneration:
    def test_single_field(self):
        result = group_by_aggregate_generation("field")
        assert result == "$field"

    def test_nested_field(self):
        result = group_by_aggregate_generation("parent.field")
        assert result == {"field": "$parent.field"}

    def test_list_of_fields(self):
        result = group_by_aggregate_generation(["field1", "field2"])
        assert result == {
            "field1": "$field1",
            "field2": "$field2"
        }

    def test_list_with_nested_fields(self):
        result = group_by_aggregate_generation(["field1", "parent.field2"])
        assert result == {
            "field1": "$field1",
            "field2": "$parent.field2"
        }


class TestClassProperty:
    def test_class_property(self):
        class TestClass:
            _value = 42

            @classproperty
            def test_property(cls):
                return cls._value

        assert TestClass.test_property == 42


class TestSortValidation:
    def test_valid_sort(self):
        result = sort_validation(('_id', 1))
        assert result == (('_id', 1),)
        result = sort_validation(('_id', -1))
        assert result == (('_id', -1),)

    def test_invalid_sort_direction(self):
        with pytest.raises(ValueError):
            sort_validation(('_id', 2))

    def test_invalid_sort_type(self):
        with pytest.raises(ValueError):
            sort_validation("invalid")


class TestGenerateNameField:
    def test_string_input(self):
        assert generate_name_field("test") == "test"

    def test_dict_input(self):
        assert generate_name_field(
            {"key1": "value1", "key2": "value2"}) == "value1|value2"

    def test_none_input(self):
        assert generate_name_field(None) is None


class TestValidateValue:
    def setup_method(self):
        class TestModel(MongoModel):
            name: str
            age: int
            active: bool

        self.TestModel = TestModel

    def test_valid_string(self):
        result = _validate_value(self.TestModel, "name", "test")
        assert result == "test"

    def test_valid_int(self):
        result = _validate_value(self.TestModel, "age", 42)
        assert result == 42

    def test_valid_bool(self):
        result = _validate_value(self.TestModel, "active", True)
        assert result is True

    def test_invalid_type(self):
        with pytest.raises(PydanticCustomError):
            _validate_value(self.TestModel, "age", "not_an_int")

    def test_nonexistent_field(self):
        with pytest.raises(PydanticCustomError):
            _validate_value(self.TestModel, "nonexistent", "value")


# class TestHandleAndConvertConnectionErrors:
#     def test_successful_operation(self):
#         @handle_and_convert_connection_errors
#         def test_func():
#             return "success"

#         assert test_func() == "success"

#     def test_connection_error(self):
#         @handle_and_convert_connection_errors
#         def test_func():
#             raise ConnectionFailure("Connection failed")

#         with pytest.raises(MongoConnectionError):
#             test_func()
