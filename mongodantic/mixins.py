from typing import Optional
from pymongo.collection import Collection

from .db import _DBConnection
from .helpers import cached_classproperty
from .querybuilder import QueryBuilder
from .types import ObjectIdStr


class ModelMixin(object):
    _id: Optional[ObjectIdStr] = None

    @cached_classproperty
    def querybuilder(cls):
        querybuilder = QueryBuilder()
        querybuilder.add_model(cls)
        return querybuilder

    @property
    def pk(self):
        return self._id

    def __hash__(self):
        if self.pk is None:
            raise TypeError("MongoModel instances without _id value are unhashable")
        return hash(self.pk)

    def __name__(self):
        return super().__name__()


class DBConnectionMixin(object):
    _connection = _DBConnection()

    @classmethod
    def _reconnect(cls):
        cls._connection = cls._connection._reconnect()

    @classmethod
    def get_database(cls):
        return cls._connection.get_database()

    @classmethod
    def set_collection_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_collection(cls) -> Collection:
        db = cls.get_database()
        return db.get_collection(cls.collection_name)

    @cached_classproperty
    def collection_name(cls):
        return cls.set_collection_name()

    @cached_classproperty
    def collection(cls):
        return cls.get_collection()
