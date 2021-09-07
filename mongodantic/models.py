import os
from json import dumps
from abc import ABC
from typing import Dict, Any, Union, Optional, List, Tuple, Set, TYPE_CHECKING
from pymongo.client_session import ClientSession
from bson import ObjectId
from pydantic.main import ModelMetaclass as PydanticModelMetaclass
from pydantic import BaseModel as BasePydanticModel
from pymongo.collection import Collection
from pymongo import IndexModel, database

from .connection import _DBConnection, _get_connection
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
from .querybuilder import QueryBuilder, AsyncQueryBuilder
from .logical import LogicalCombination, Query
from .connection import get_connection_env

if TYPE_CHECKING:
    from pydantic.typing import DictStrAny
    from pydantic.typing import AbstractSetIntStr  # noqa: F401

__all__ = ('MongoModel', 'QuerySet', 'Query')

_is_mongo_model_class_defined = False


class ModelMetaclass(PydanticModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        indexes = set()
        if _is_mongo_model_class_defined and issubclass(cls, MongoModel):
            querybuilder = getattr(cls, '__querybuilder__')
            async_querybuilder = getattr(cls, '__aquerybuilder__')
            if querybuilder is None:
                querybuilder = QueryBuilder(cls)
                setattr(cls, '__querybuilder__', querybuilder)
            if async_querybuilder is None:
                async_querybuilder = AsyncQueryBuilder(cls)
                setattr(cls, '__aquerybuilder__', async_querybuilder)
            # setattr(cls, 'querybuilder', querybuilder)

        exclude_fields = getattr(cls.Config, 'exclude_fields', tuple())
        setattr(cls, '__indexes__', indexes)
        setattr(cls, '__exclude_fields__', exclude_fields)
        return cls


class BaseModel(ABC, BasePydanticModel, metaclass=ModelMetaclass):
    __indexes__: Set['str'] = set()
    __exclude_fields__: Union[Tuple, List] = tuple()
    __connection__: Optional[_DBConnection] = None
    __querybuilder__: Optional[QueryBuilder] = None
    __aquerybuilder__: Optional[AsyncQueryBuilder] = None
    _id: Optional[ObjectIdStr] = None

    def __setattr__(self, key, value):
        if key in self.__fields__:
            return super().__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def _get_properties(cls):
        return [
            prop
            for prop in dir(cls)
            if prop
            not in (
                "__values__",
                "fields",
                "data",
                "_connection",
                "_collection_name",
                "_collection",
                "querybuilder",
                "pk",
                "query_data",
                "all_fields",
            )
            and isinstance(getattr(cls, prop), property)
        ]

    @classmethod
    def parse_obj(cls, data: Any) -> Any:
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = data['_id']
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

    @classproperty
    def all_fields(cls) -> list:
        fields = list(cls.__fields__.keys())
        return_fields = fields + cls._get_properties()
        return return_fields

    @classmethod
    def _check_query_args(
        cls,
        logical_query: Union[
            List[Any], Dict[Any, Any], str, Query, LogicalCombination
        ] = None,
    ) -> Dict:
        if not isinstance(logical_query, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical_query.to_query(cls)

    @classmethod
    def _start_session(cls) -> ClientSession:
        client = cls._connection._mongo_connection
        return client.start_session()

    @classmethod
    def sort_fields(cls, fields: Union[Tuple, List, None]) -> None:
        if fields:
            new_sort = {field: cls.__fields__[field] for field in fields}
            cls.__fields__ = new_sort

    def dict(  # type: ignore
        self,
        *,
        include: Union['AbstractSetIntStr'] = None,
        exclude: Union['AbstractSetIntStr'] = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        with_props: bool = True,
    ) -> 'DictStrAny':
        attribs = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        if with_props:
            props = self._get_properties()
            # Include and exclude properties
            if include:
                props = [prop for prop in props if prop in include]
            if exclude:
                props = [prop for prop in props if prop not in exclude]

            # Update the attribute dict with the properties
            if props:
                attribs.update({prop: getattr(self, prop) for prop in props})

        return attribs

    def _data(self, with_props: bool = True) -> Dict:
        data = self.dict(with_props=with_props)
        if '_id' in data:
            data['_id'] = data['_id'].__str__()
        return data

    @property
    def data(self) -> Dict:
        return self._data(with_props=True)

    @property
    def query_data(self) -> Dict:
        return self._data(with_props=False)

    @classmethod
    def _get_connection(cls) -> _DBConnection:
        return _get_connection(alias=str(os.getpid()), env_name=get_connection_env())

    @classproperty
    def _connection(cls) -> Optional[_DBConnection]:
        if not cls.__connection__ or cls.__connection__._alias != str(os.getpid()):
            cls.__connection__ = cls._get_connection()
        return cls.__connection__

    @classmethod
    def get_database(cls) -> database.Database:
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
    def q(cls) -> Optional[QueryBuilder]:
        return cls.__querybuilder__

    @classproperty
    def aq(cls) -> Optional[AsyncQueryBuilder]:
        return cls.__aquerybuilder__

    @classproperty
    def querybuilder(cls) -> Optional[QueryBuilder]:
        return cls.q

    @classproperty
    def async_querybuilder(cls) -> Optional[AsyncQueryBuilder]:
        return cls.aq

    @classmethod
    def execute_indexes(cls):
        indexes = getattr(cls.__config__, 'indexes', [])
        if not all([isinstance(index, IndexModel) for index in indexes]):
            raise ValueError('indexes must be list of IndexModel instances')
        if indexes:
            db_indexes = cls.q.check_indexes()
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
                result = cls.q.create_indexes(indexes_to_create)
            if indexes_to_delete:
                for index_name in indexes_to_delete:
                    cls.q.drop_index(index_name)
                db_indexes = cls.q.check_indexes()
            indexes = set(list(db_indexes.keys()) + result)
        setattr(cls, '__indexes__', indexes)

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
            self.q.update_one(
                session=session, **data,
            )
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in self.__fields__
        }
        object_id = self.q.insert_one(session=session, **data,)
        self._id = object_id.__str__()
        return self

    def delete(self, session: Optional[ClientSession] = None) -> None:
        self.q.delete_one(_id=ObjectId(self._id), session=session)

    def drop(self, session: Optional[ClientSession] = None) -> None:
        return self.delete(session)

    def serialize(self, fields: Union[Tuple, List]) -> dict:
        data = self.dict(include=set(fields))
        return {f: data[f] for f in fields}

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return dumps(self.serialize(fields))


class MongoModel(BaseModel):
    def __hash__(self):
        if self.pk is None:
            raise TypeError("MongoModel instances without _id value are unhashable")
        return hash(self.pk)

    @property
    def pk(self):
        return self._id


_is_mongo_model_class_defined = True
