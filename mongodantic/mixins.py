from pymongo.collection import Collection

from .db import DBConnection
from .helpers import cached_classproperty


class DBMixin(object):
    _connection = DBConnection()

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
