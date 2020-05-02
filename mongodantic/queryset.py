from typing import Generator, List, Optional
from pydantic.main import ModelMetaclass


from .helpers import handle_and_convert_connection_errors


class QuerySet(object):
    def __init__(
        self,
        model: ModelMetaclass,
        data: Generator,
        reference_model: Optional[ModelMetaclass] = None,
    ):
        self._data = data
        self._model = model
        self._reference_model = reference_model

    @handle_and_convert_connection_errors
    def __iter__(self):
        for obj in self._data:
            yield self._model.parse_obj(obj, self._reference_model)

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
