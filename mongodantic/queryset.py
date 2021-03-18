from json import dumps
from typing import Generator, List, Optional, Union, Any
from pydantic.main import ModelMetaclass


from .helpers import handle_and_convert_connection_errors


class QuerySet(object):
    def __init__(
        self,
        model: ModelMetaclass,
        data: Generator,
        reference_models: Optional[List[ModelMetaclass]] = None,
    ):
        self._data = data
        self._model = model
        self._reference_models = reference_models

    @handle_and_convert_connection_errors
    def __iter__(self):
        for obj in self._data:
            yield self._model.parse_obj(obj, self._reference_models)

    def __next__(self):
        return next(self.__iter__())

    @property
    def data(self) -> List:
        return [obj.data for obj in self.__iter__()]

    @property
    def json(self) -> str:
        return dumps(self.data)

    @property
    def generator(self) -> Generator:
        return self.__iter__()

    @property
    def data_generator(self) -> Generator:
        return (obj.data for obj in self.__iter__())

    @property
    def list(self) -> List:
        return list(self.__iter__())

    def first(self) -> Any:
        return next(self.__iter__())

    def serialize(self, fields: Union[tuple, List]) -> List:
        return [obj.serialize(fields) for obj in self.__iter__()]

    def serialize_generator(self, fields: Union[tuple, List]) -> Generator:
        for obj in self.__iter__():
            yield obj.serialize(fields)

    def serialize_json(self, fields: Union[tuple, List]) -> str:
        return dumps(self.serialize(fields))
