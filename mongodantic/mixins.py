from .db import DBConnection


class DBMixin(object):
    class _Meta:
        _connection = DBConnection()
        _database = _connection.database

    @classmethod
    def _reconnect(cls):
        connection = DBConnection()
        cls._Meta._connection = connection
        cls._Meta._database = connection.database

