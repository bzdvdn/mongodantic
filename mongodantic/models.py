import os
from json import dumps
from typing import Dict, Any, Union, Optional, List, Tuple, Set
from pymongo.client_session import ClientSession
from bson import ObjectId
from pydantic.main import ModelMetaclass as PydanticModelMetaclass
from pydantic import BaseModel as BasePydanticModel
from pymongo.collection import Collection
from pymongo import IndexModel

from .db import _DBConnection, _get_connection
from .types import ObjectIdStr
from .exceptions import (
    NotDeclaredField,
    ValidationError,
    InvalidArgsParams,
)
from .helpers import (
    ExtraQueryMapper,
    classproperty,
    _validate_value,
)
from .querybuilder import QueryBuilder
from .logical import LogicalCombination, Query
from .connection import get_connection_env


__all__ = ('MongoModel', 'QuerySet', 'Query')

_is_mongo_model_class_defined = False


class ModelMetaclass(PydanticModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        indexes = set()
        if _is_mongo_model_class_defined and issubclass(cls, MongoModel):
            querybuilder = getattr(cls, '__querybuilder__')
            if querybuilder is None:
                querybuilder = QueryBuilder()
                setattr(cls, '__querybuilder__', querybuilder)
            querybuilder.add_model(cls)
            # setattr(cls, 'querybuilder', querybuilder)
            indexes = getattr(cls.__config__, 'indexes', [])
            if not all([isinstance(index, IndexModel) for index in indexes]):
                raise ValueError('indexes must be list of IndexModel instances')
            if indexes:
                db_indexes = cls.querybuilder.check_indexes()
                indexes_to_create = [
                    i for i in indexes if i.document['name'] not in db_indexes
                ]
                indexes_to_delete = [
                    i
                    for i in db_indexes
                    if i not in [i.document['name'] for i in indexes] and i != '_id_'
                ]
                result = []
                if indexes_to_create:
                    result = cls.__querybuilder__.create_indexes(indexes_to_create)
                if indexes_to_delete:
                    for index_name in indexes_to_delete:
                        cls.__querybuilder__.drop_index(index_name)
                    db_indexes = cls.__querybuilder__.check_indexes()
                indexes = set(list(db_indexes.keys()) + result)
        exclude_fields = getattr(cls.Config, 'exclude_fields', tuple())
        setattr(cls, '__indexes__', indexes)
        setattr(cls, '__exclude_fields__', exclude_fields)
        return cls


class BaseModel(BasePydanticModel, metaclass=ModelMetaclass):
    __indexes__: Set['str'] = set()
    __exclude_fields__: Union[Tuple, List] = tuple()
    __connection__: Optional[_DBConnection] = None
    __querybuilder__: Optional[QueryBuilder] = None

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
        elif field in cls.__exclude_fields__:
            return False
        return True

    @classmethod
    def _parse_extra_params(cls, extra_params: List) -> tuple:
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
            extra = ExtraQueryMapper(cls, field).extra_query(extra_params, value)
            if extra:
                value = extra[field]
            elif field == '_id':
                value = ObjectId(value)
            else:
                value = _validate_value(cls, field, value) if not inners else value
            if inners:
                field = f'{field}.{".".join(i for i in inners)}'
            data[field] = value
        return data

    @classmethod
    def _check_query_args(
        cls, logical_query: Union[Query, LogicalCombination, None] = None
    ) -> Dict:
        if not isinstance(logical_query, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical_query.to_query(cls)

    @classmethod
    def _start_session(cls) -> ClientSession:
        client = cls._connection._mongo_connection
        return client.start_session()

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

    @classmethod
    def _get_connection(cls):
        return _get_connection(alias=str(os.getpid()), env_name=get_connection_env())

    @classproperty
    def _connection(cls):
        if not cls.__connection__ or cls.__connection__._alias != str(os.getpid()):
            cls.__connection__ = cls._get_connection()
        return cls.__connection__

    @classmethod
    def get_database(cls):
        return cls._connection.get_database()

    @classmethod
    def set_collection_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_collection(cls) -> Collection:
        db = cls.get_database()
        return db.get_collection(cls._collection_name)

    @classmethod
    def _reconnect(cls):
        if cls.__connection__:
            cls.__connection__ = cls.__connection__._reconnect()
        cls.__connection__ = cls._get_connection()

    @classproperty
    def _collection_name(cls) -> str:
        return cls.set_collection_name()

    @classproperty
    def _collection(cls) -> Collection:
        return cls.get_collection()

    @classproperty
    def querybuilder(cls) -> Optional[QueryBuilder]:
        return cls.__querybuilder__


class MongoModel(BaseModel):
    _id: Optional[ObjectIdStr] = None

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
            self.querybuilder.update_one(
                session=session, **data,
            )
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in self.__fields__
        }
        object_id = self.querybuilder.insert_one(session=session, **data,)
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

    @property
    def pk(self):
        return self._id


_is_mongo_model_class_defined = True
