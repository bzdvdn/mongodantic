from typing import Any
from contextlib import ContextDecorator

__all__ = ("Session",)


class Session(ContextDecorator):
    def __init__(self, mongo_model: Any):
        self._mongo_model = mongo_model
        self._session = self._mongo_model._start_session()

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        self._session.end_session()
