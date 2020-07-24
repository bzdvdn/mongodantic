from typing import Any
from contextlib import ContextDecorator

from .db import _DBConnection

__all__ = ("Session",)


class Session(ContextDecorator):
    _connection = _DBConnection()

    def __init__(self):
        self._session = self._connection._mongo_connection.start_session()

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return self.close()

    def close(self):
        return self._session.end_session()
