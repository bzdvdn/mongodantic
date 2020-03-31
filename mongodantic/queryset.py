from typing import Generator, List
from pydantic.main import ModelMetaclass
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure, \
    WriteConcernError

from .exceptions import MongoConnectionError


class QuerySet(object):
    def __init__(self, model: ModelMetaclass, data: Generator):
        self._data = data
        self._model = model

    def __iter__(self):
        try:
            for obj in self._data:
                yield self._model.parse_obj(obj)
        except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure, WriteConcernError) as e:
            raise MongoConnectionError(str(e))

    @property
    def data(self) -> List:
        return [obj.data for obj in self.__iter__()]

    @property
    def generator(self) -> Generator:
        return self.__iter__()

    @property
    def data_generator(self) -> Generator:
        return (obj.data for obj in self.__iter__())

    @property
    def list(self) -> List:
        return list(self.__iter__())

    def first(self) -> any:
        return next(self.__iter__())
