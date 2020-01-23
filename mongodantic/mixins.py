from .db import DBConnection


class DBMixin(object):
    class _Meta:
        _database = DBConnection().database

