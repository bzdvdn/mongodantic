from typing import Generator


class QuerySet(object):
    def __init__(self, data: Generator):
        self._data = data

    def __iter__(self):
        return (obj for obj in self._data)

    @property
    def data(self) -> Generator:
        return (obj.data for obj in self._data)

    @property
    def list(self) -> list:
        return [obj for obj in self._data]

    def first(self) -> any:
        return next(self.__iter__())
