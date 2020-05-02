from .db import DBConnection


class DBMixin(object):
    _connection = DBConnection()

    @classmethod
    def _reconnect(cls):
        cls._connection = cls._connection._reconnect()

    @classmethod
    def get_database(cls):
        return cls._connection.get_database()
