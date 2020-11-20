from contextlib import ContextDecorator

__all__ = ("Session",)


class Session(ContextDecorator):
    def __init__(self, model):
        self._session = model._start_session()

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return self.close()

    def close(self):
        return self._session.end_session()
