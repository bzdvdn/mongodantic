from .db import DBConnection


class DBMixin(object):
    class _Meta:
        _connection = DBConnection()
        _database = _connection.database

    @classmethod
    def _reconnect(cls):
        cls._Meta._connection = cls._Meta._connection._reconnect()
        cls._Meta._database = cls._Meta._connection.database

