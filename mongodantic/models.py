from json import dumps
from typing import Dict, Any, Union, Optional, List, Tuple
from pymongo.client_session import ClientSession
from bson import ObjectId
from pydantic.main import ModelMetaclass
from pydantic import BaseModel as BasePydanticModel
from pydantic import validator

from .mixins import DBConnectionMixin, ModelMixin
from .types import ObjectIdStr
from .exceptions import (
    NotDeclaredField,
    ValidationError,
    InvalidArgsParams,
)
from .helpers import ExtraQueryMapper
from .queryset import QuerySet
from .logical import LogicalCombination, Query


__all__ = ('MongoModel', 'QuerySet', 'Query')


class BaseModel(DBConnectionMixin, ModelMixin, BasePydanticModel):
    class Config:
        excluded_query_fields = ()

    def __setattr__(self, key, value):
        if key in self.__fields__:
            return super().__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def parse_obj(
        cls, data: Any, reference_models: Dict[Any, 'BaseModel'] = {},
    ) -> Any:
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = data['_id']
        if reference_models:
            obj = cls.__set_reference_fields(obj, data, reference_models)
        return obj

    @classmethod
    def __set_reference_fields(
        cls, obj: 'BaseModel', data: Dict, reference_models: Dict[Any, 'BaseModel'],
    ) -> 'BaseModel':
        for name_as, ref in reference_models.items():
            data = data[name_as]
            if isinstance(data, dict):
                ref_obj = ref.parse_obj(data)
            else:
                ref_obj = [ref.parse_obj(d) for d in data]
            setattr(obj, f'{name_as}', ref_obj)
        return obj

    @classmethod
    def __validate_field(cls, field: str) -> bool:
        if field not in cls.__fields__ and field != '_id':
            raise NotDeclaredField(field, list(cls.__fields__.keys()))
        elif field in cls.Config.excluded_query_fields:
            return False
        return True

    @classmethod
    def _parse_extra_params(cls, extra_params: tuple) -> tuple:
        field_param, extra = [], []
        methods = ExtraQueryMapper.methods
        for param in extra_params:
            if param in methods:
                extra.append(param)
            else:
                field_param.append(param)
        return field_param, extra

    @classmethod
    def _validate_query_data(cls, query: Dict) -> Dict:
        data = {}
        for field, value in query.items():
            field, *extra_params = field.split("__")
            inners, extra_params = cls._parse_extra_params(extra_params)
            if not cls.__validate_field(field):
                continue
            _dict = ExtraQueryMapper(field).extra_query(extra_params, value)
            if _dict:
                value = _dict[field]
            elif field == '_id':
                value = ObjectId(value)
            else:
                value = cls.__validate_value(field, value) if not inners else value
            if inners:
                field = f'{field}.{".".join(i for i in inners)}'
            data[field] = value
        return data

    @classmethod
    def __validate_value(cls, field_name: str, value: Any) -> Any:
        field = cls.__fields__[field_name]
        if isinstance(field, ObjectIdStr):
            try:
                value = field.validate(value)
            except ValueError as e:
                error_ = e
        else:
            value, error_ = field.validate(value, {}, loc=field.alias, cls=cls)
        if error_:
            raise ValidationError([error_], type(value))
        return value

    @classmethod
    def _check_query_args(
        cls, logical_query: Union[Query, LogicalCombination, None] = None
    ) -> Dict:
        if not isinstance(logical_query, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical_query.to_query(cls)

    @classmethod
    def _start_session(cls) -> ClientSession:
        return cls._connection._mongo_connection.start_session()

    @classmethod
    def sort_fields(cls, fields: Union[tuple, list, None]) -> None:
        if fields:
            new_sort = {field: cls.__fields__[field] for field in fields}
            cls.__fields__ = new_sort

    @property
    def data(self) -> Dict:
        data = self.dict()
        if '_id' in data:
            data['_id'] = data['_id'].__str__()
        return data


class MongoModel(BaseModel):
    def save(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[ClientSession] = None,
    ) -> Any:
        if self._id is not None:
            data = {'_id': ObjectId(self._id)}
            if updated_fields:
                if not all(field in self.__fields__ for field in updated_fields):
                    raise ValidationError('invalid field in updated_fields')
            else:
                updated_fields = tuple(self.__fields__.keys())
            for field in updated_fields:
                data[f'{field}__set'] = getattr(self, field)
            self.querybuilder.update_one(**data, session=session)
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in self.__fields__
        }
        object_id = self.querybuilder.insert_one(**data, session=session)
        self._id = object_id.__str__()
        return self

    def delete(self, session: Optional[ClientSession] = None) -> None:
        self.querybuilder.delete_one(_id=ObjectId(self._id), session=session)

    def drop(self, session: Optional[ClientSession] = None) -> None:
        return self.delete(session)

    def serialize(self, fields: Union[tuple, list]) -> dict:
        data = self.dict(include=set(fields))
        return {f: data[f] for f in fields}

    def serialize_json(self, fields: Union[tuple, list]) -> str:
        return dumps(self.serialize(fields))

    def __hash__(self):
        if self.pk is None:
            raise TypeError("MongoModel instances without _id value are unhashable")
        return hash(self.pk)
