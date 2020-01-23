import os
from typing import TYPE_CHECKING, Dict, Any, Set, List, Generator, Union
from pymongo.collection import Collection
from bson import ObjectId
from pydantic import validate_model
from pydantic.main import ModelMetaclass

from .mixins import DBMixin
from .types import ObjectIdStr
from .exceptions import NotDeclaredField, InvalidArgument

SetStr = Set[str]


class BaseModel(DBMixin):
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
        if key not in ('_id',):
            return super(BaseModel, self).__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def parse_obj(cls, data):
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = str(data['_id'])
        return obj

    @classmethod
    def __validate_query_data(cls, query: dict) -> dict:
        data = {}
        for field, value in query.items():
            if field == '_id':
                value = ObjectId(value)
            elif field not in cls.__fields__:
                raise NotDeclaredField(field, list(cls.__fields__.keys()))
            data[field] = value
        return data

    @classmethod
    def __query(cls, method_name: str, query_params: Union[list, dict]) -> Any:
        if isinstance(query_params, dict):
            query_params = cls.__validate_query_data(query_params)
        if not hasattr(cls, 'collection'):
            cls.collection = cls._Meta._database.get_collection(cls.__name__)
        return cls.collection.__getattribute__(method_name)(query_params)

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
    def find(cls, **query) -> Generator:
        data = cls.__query('find', query)
        return (cls.parse_obj(obj) for obj in data)

    @classmethod
    def insert_one(cls, **query) -> int:
        data = cls.__query('insert_one', query)
        return data.inserted_id

    @classmethod
    def insert_many(cls, data: List) -> int:
        query = []
        for obj in data:
            if not isinstance(obj, ModelMetaclass):
                raise InvalidArgument()
            query.append(obj.data)
        r = cls.__query('insert_many', query)
        return r.inserted_ids

    @classmethod
    def delete_one(cls, **query) -> int:
        r = cls.__query('delete_one', query)
        return r.deleted_count

    @classmethod
    def delete_many(cls, **query):
        r = cls.__query('delete_many', query)
        return r.deleted_count

    @property
    def data(self) -> dict:
        return self.dict()
