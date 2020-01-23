from .db import DBConnection


class DBMixin(object):
    class _Meta:
        _connection = DBConnection()
        _database = _connection.database

