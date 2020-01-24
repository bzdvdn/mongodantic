import os
from typing import TYPE_CHECKING, Dict, Any, Set, List, Generator, Union, Optional
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from bson import ObjectId
from pydantic import validate_model
from pydantic.main import ModelMetaclass
from pydantic import BaseModel

from .mixins import DBMixin
from .types import ObjectIdStr
from .exceptions import NotDeclaredField, InvalidArgument, ValidationError, MongoIndexError
from .helpers import ExtraQueryMapper, chunk_by_length, bulk_update_query_generator
from .queryset import QuerySet

SetStr = Set[str]

__all__ = ('MongoModel', 'QuerySet')


class MongoModel(DBMixin, BaseModel):
    _id: ObjectIdStr = None

    def __init__(self, **data):
        if TYPE_CHECKING:
            self.__dict__: Dict[str, Any] = {}
            self.__fields_set__: 'SetStr' = set()
        if data:
            values, fields_set, validation_error = validate_model(self.__class__, data)
            if validation_error:
                raise validation_error
            object.__setattr__(self, '__dict__', values)
            object.__setattr__(self, '__fields_set__', fields_set)

    def __setattr__(self, key, value):
        if key != '_id':
            return super(MongoModel, self).__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def parse_obj(cls, data):
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = str(data['_id'])
        return obj

    @classmethod
    def __validate_query_data(cls, query: dict, value_validation: bool = False) -> dict:
        data = {}
        for field, value in query.items():
            if '__' in field:
                extra_fields = field.split("__")
                field = extra_fields[0]
                extra_param = extra_fields[1]
                _dict = ExtraQueryMapper(field).extra_query(extra_param, value)
                value = _dict[field]
            elif field not in cls.__fields__:
                raise NotDeclaredField(field, list(cls.__fields__.keys()))
            data[field] = value if not value_validation else cls.__validate_value(field, value)
        return data

    @classmethod
    def __validate_value(cls, field_name: str, value: Any) -> Any:
        field_type = cls.__fields__[field_name].type_
        try:
            value = field_type(value)
            return value
        except ValueError:
            raise ValidationError(f"{field_name} cant be converter to {field_type.__name__}, with value - {value}")

    @classmethod
    def __query(cls, method_name: str, query_params: Union[list, dict, str], set_values: Optional[Dict] = None,
                background: Optional[bool] = None) -> Any:
        if isinstance(query_params, dict):
            query_params = cls.__validate_query_data(query_params)
        if not hasattr(cls, 'collection'):
            cls.collection = cls._Meta._database.get_collection(cls.__name__)
        if set_values:
            return cls.collection.__getattribute__(method_name)(query_params, set_values)
        if background is not None:
            return cls.collection.__getattribute__(method_name)(query_params, background=background)
        return cls.collection.__getattribute__(method_name)(query_params)

    @classmethod
    def check_indexes(cls) -> dict:
        index_list = list(cls.__query('list_indexes', {}))
        return_dict = {}
        for index in index_list:
            d = dict(index)
            _dict = {'name': d['name'], 'key': dict(d['key'])}
            return_dict.update(_dict)
        return return_dict

    @classmethod
    def add_index(cls, index_name: str, index_type: int, background: bool = True) -> str:
        indexes = cls.check_indexes()
        if index_name in indexes:
            raise MongoIndexError(f'{index_name} - already exists.')
        try:
            cls.__query('create_index', [(index_name, index_type)], background=background)
            return f'index with name - {index_name} created.'
        except Exception as e:
            raise MongoIndexError(f'unknown error, detail: {str(e)}')

    @classmethod
    def drop_index(cls, index_name: str) -> str:
        cls.__query('drop_index', index_name)
        return f'{index_name} dropped.'

    @classmethod
    def count(cls, **query) -> int:
        return cls.__query('count', query)

    @classmethod
    def find_one(cls, **query) -> Any:
        data = cls.__query('find_one', query)
        if data:
            obj = cls.parse_obj(data)
            return obj
        return None

    @classmethod
    def find(cls, **query) -> QuerySet:
        data = cls.__query('find', query)
        return QuerySet((cls.parse_obj(obj) for obj in data))

    @classmethod
    def find_with_limit(cls, limit: int = 100, **query):
        data = cls.__query('find', query).limit(limit)
        return QuerySet((cls.parse_obj(obj) for obj in data))

    @classmethod
    def insert_one(cls, **query) -> ObjectId:
        obj = cls.parse_obj(query)
        data = cls.__query('insert_one', obj.data)
        return data.inserted_id

    @classmethod
    def insert_many(cls, data: List) -> int:
        query = []
        for obj in data:
            if not isinstance(obj, ModelMetaclass):
                obj = cls.parse_obj(obj)
            query.append(obj.data)
        r = cls.__query('insert_many', query)
        return len(r.inserted_ids)

    @classmethod
    def delete_one(cls, **query) -> int:
        r = cls.__query('delete_one', query)
        return r.deleted_count

    @classmethod
    def delete_many(cls, **query):
        r = cls.__query('delete_many', query)
        return r.deleted_count

    @classmethod
    def _ensure_update_data(cls, **fields) -> tuple:
        if not any("__set" in f for f in fields):
            raise AttributeError("not fields for updating!")
        queries = {}
        set_values = {}
        for name, value in fields.items():
            extra_fields = name.split("__")
            if len(extra_fields) == 2:
                if extra_fields[1] == "set":
                    _dict = cls.__validate_query_data({extra_fields[0]: value}, value_validation=True)
                    set_values.update(_dict)
            else:
                _dict = cls.__validate_query_data({name: value})
                queries.update(_dict)
        return queries, set_values

    @classmethod
    def _update(cls, method: str, query: dict) -> int:
        query, set_values = cls._ensure_update_data(**query)
        r = cls.__query(method, query, {'$set': set_values})
        return r.modified_count

    @classmethod
    def update_one(cls, **query) -> int:
        return cls._update('update_one', query)

    @classmethod
    def update_many(cls, **query) -> int:
        return cls._update('update_many', query)

    @classmethod
    def _aggregate(cls, operation: str, agg_field: str, **query) -> int:
        query = cls.__validate_query_data(query)
        data = [
            {"$match": query},
            {"$group": {"_id": None, "total": {f"${operation}": f"${agg_field}"}}},
        ]
        try:
            return cls.__query("aggregate", data).next()["total"]
        except StopIteration:
            return 0

    @classmethod
    def bulk_update(cls, models: List, updated_fields: list, batch_size: Optional[int] = None) -> None:
        if batch_size is not None and batch_size > 0:
            for requests in chunk_by_length(models, batch_size):
                data = bulk_update_query_generator(requests, updated_fields)
                cls.__query('bulk_write', data)
        data = bulk_update_query_generator(models, updated_fields)
        cls.__query('bulk_write', data)

    @classmethod
    def aggregate_sum(cls, agg_field: str, **query) -> int:
        return cls._aggregate('sum', agg_field, **query)

    @classmethod
    def aggregate_max(cls, agg_field: str, **query) -> int:
        return cls._aggregate('max', agg_field, **query)

    @classmethod
    def aggregate_min(cls, agg_field: str, **query) -> int:
        return cls._aggregate('min', agg_field, **query)

    @property
    def data(self) -> dict:
        return self.dict()
