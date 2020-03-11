from typing import Any
from contextlib import ContextDecorator

__all__ = ("SessionManager",)


class SessionManager(ContextDecorator):
    def __init__(self, mongo_model: Any):
        self._mongo_model = mongo_model
        self._session = self._mongo_model._Meta._connection._mongo_connection.start_session()

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        self._session.end_session()
