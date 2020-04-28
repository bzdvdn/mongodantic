from .db import DBConnection


class DBMixin(object):
    class _Meta:
        _connection = DBConnection()

    @classmethod
    def _reconnect(cls):
        cls._Meta._connection = cls._Meta._connection._reconnect()

