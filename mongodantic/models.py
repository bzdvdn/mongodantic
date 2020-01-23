import os
from typing import TYPE_CHECKING, Dict, Any, Set
from pymongo.collection import Collection
from pydantic import validate_model

from .db import DBConnection
from .mixins import DBMixin
from .types import ObjectIdStr
from .exceptions import NotDeclaredField

SetStr = Set[str]


class BaseModel(DBMixin):
    _id: ObjectIdStr = None

    class _Meta:
        _database = DBConnection().database

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

    @classmethod
    def __validate_query_data(cls, query: dict) -> dict:
        data = {}
        for field, value in query.items():
            if field not in cls.__fields__:
                raise NotDeclaredField(field, list(cls.__fields__.keys()))
            data[field] = value
        return data

    @classmethod
    def __query(cls, method_name: str, query_params: dict):
        query_params = cls.__validate_query_data(query_params)
        if not hasattr(cls, 'collection'):
            cls.collection = cls._Meta._database.get_collection(cls.__name__)
        return cls.collection.__getattribute__(method_name)(query_params)

    @classmethod
    def count(cls) -> int:
        return cls.__query('count', {})

    @classmethod
    def find_one(cls, **query):
        data = cls.__query('find_one', query)
        if data:
            return cls(**data)
        return None

    @classmethod
    def insert_one(cls, **query):
        data = cls.__query('insert_one', query)
        return data
