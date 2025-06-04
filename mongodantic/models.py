import os
import json
from logging import getLogger
from typing import Dict, Any, Union, Optional, List, Tuple, Set, TYPE_CHECKING, ClassVar
from pymongo.client_session import ClientSession
from bson import ObjectId
from pydantic import BaseModel, ConfigDict
from pymongo.collection import Collection
from pymongo import IndexModel, database

from .connection import _DBConnection, _get_connection
from .types import ObjectIdStr
from .exceptions import (
    NotDeclaredField,
    MongoValidationError,
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

__all__ = ('MongoModel', 'Query')

logger = getLogger('mongodantic')

_is_mongo_model_class_defined = False


class MongoModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: lambda f: str(f)},
        validate_assignment=True
    )

    __indexes__: ClassVar[Set[str]] = set()
    __mongo_exclude_fields__: ClassVar[Union[Tuple, List]] = tuple()
    __connection__: ClassVar[Optional[_DBConnection]] = None
    __querybuilder__: ClassVar[Optional[QueryBuilder]] = None
    __async_querybuilder__: ClassVar[Optional[AsyncQueryBuilder]] = None
    _id: Optional[ObjectIdStr] = None

    def __init__(self, **data):
        super().__init__(**data)
        if not hasattr(self.__class__, '__querybuilder__'):
            self.__class__.__querybuilder__ = QueryBuilder(self.__class__)

    def __setattr__(self, key, value):
        if key in type(self).model_fields:
            return super().__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def _get_properties(cls) -> list:
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
                "Q",
                "AQ",
                "async_querybuilder",
                "pk",
                "query_data",
                "fields_all",
                "all_fields",
                "model_fields_set",
            )
            and isinstance(getattr(cls, prop), property)
        ]

    @classmethod
    def model_validate(cls, data: Any) -> Any:
        obj = super().model_validate(data)
        if '_id' in data:
            obj._id = data['_id']
        return obj

    @classmethod
    def __validate_field(cls, field: str) -> bool:
        if not field:  # Handle empty field names
            return True
        # Skip Pydantic internal fields
        if field.startswith('model_'):
            return True
        # Handle special suffixes like __set, __in, etc.
        base_field = field.split('__')[0]
        if base_field not in cls.model_fields and base_field != '_id':
            raise NotDeclaredField(base_field, list(cls.model_fields.keys()))
        elif base_field in cls.__mongo_exclude_fields__:
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
    def _validate_query_data(cls, query_params: Dict) -> Dict:
        """Validate query data

        Args:
            query_params: query parameters

        Returns:
            Dict: validated query parameters
        """
        validated_query = {}
        for field_name, value in query_params.items():
            # Skip validation for MongoDB operators
            if field_name.startswith('$') or (isinstance(value, dict) and any(k.startswith('$') for k in value)):
                validated_query[field_name] = value
                continue

            if '__' in field_name:
                base_field, *extra_params = field_name.split('__')
                if base_field not in cls.model_fields:
                    continue
                validated_query.update(
                    ExtraQueryMapper(
                        cls, base_field).extra_query(extra_params, value))
            else:
                validated_query[field_name] = _validate_value(
                    cls, field_name, value)
        return validated_query

    @classproperty
    def fields_all(cls) -> list:
        fields = list(cls.model_fields.keys())
        return_fields = fields + cls._get_properties()
        return return_fields

    @classmethod
    def _check_query_args(
        cls,
        logical_query: Union[
            List[Any], Dict[Any, Any], str, Query, LogicalCombination, None
        ] = None,
    ) -> 'DictStrAny':
        """check if query = Query obj or LogicCombination

        Args:
            logical_query (Union[ List[Any], Dict[Any, Any], str, Query, LogicalCombination ], optional): Query | LogicCombination. Defaults to None.

        Raises:
            InvalidArgsParams: if not Query | LogicCombination

        Returns:
            Dict: generated query dict
        """
        if not isinstance(logical_query, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical_query.to_query(cls)  # type: ignore

    @classmethod
    def _start_session(cls) -> ClientSession:
        client = cls._connection._mongo_connection
        return client.start_session()

    @classmethod
    def sort_fields(cls, fields: Union[Tuple, List, None]) -> None:
        if fields:
            new_sort = {field: cls.model_fields[field] for field in fields}
            cls.model_fields = new_sort

    def model_dump(
        self,
        *,
        include: Optional['AbstractSetIntStr'] = None,
        exclude: Optional['AbstractSetIntStr'] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        with_props: bool = True,
    ) -> 'DictStrAny':
        # Add internal Pydantic fields to exclude
        exclude = set(exclude or set())
        exclude.update({
            'model_fields_set',
            'model_extra',
            'model_fields',
            'model_config',
            'model_post_init',
            'model_validate',
            'model_validate_json',
            'model_dump',
            'model_dump_json',
            'model_copy',
            'model_construct',
            'model_parse_obj',
            'model_parse_raw',
            'model_validate_json_file',
            'model_validate_json_str',
            'model_validate_json_bytes'
        })

        data = super().model_dump(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )

        # Convert any sets to lists for MongoDB compatibility
        for key, value in data.items():
            if isinstance(value, set):
                data[key] = list(value)

        if with_props:
            for prop in self._get_properties():
                value = getattr(self, prop)
                if isinstance(value, set):
                    value = list(value)
                data[prop] = value

        return data

    def _data(self, with_props: bool = True) -> 'DictStrAny':
        return self.model_dump(with_props=with_props)

    @property
    def data(self) -> 'DictStrAny':
        return self._data()

    @property
    def query_data(self) -> 'DictStrAny':
        return self._data()

    @classmethod
    def _get_connection(cls) -> _DBConnection:
        return _get_connection(str(os.getpid()))

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
        """main method for set collection

        Returns:
            str: collection name
        """
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
    def Q(cls) -> Optional[QueryBuilder]:
        return cls.__querybuilder__

    @classproperty
    def AQ(cls) -> Optional[AsyncQueryBuilder]:
        return cls.__async_querybuilder__

    @classproperty
    def querybuilder(cls) -> Optional[QueryBuilder]:
        logger.warning('querybuilder property is deprecated.')
        return cls.Q

    @classproperty
    def async_querybuilder(cls) -> Optional[AsyncQueryBuilder]:
        logger.warning('async_querybuilder property is deprecated.')
        return cls.AQ

    @classmethod
    def execute_indexes(cls):
        """method for create/update/delete indexes if indexes declared in Config property"""
        indexes = getattr(cls.model_config, 'indexes', [])
        if not all([isinstance(index, IndexModel) for index in indexes]):
            raise ValueError('indexes must be list of IndexModel instances')
        if indexes:
            db_indexes = cls.Q.check_indexes()
            # Get index names from the index models
            index_names = []
            for index in indexes:
                # Get the first field name and direction from the index model
                key = dict(index.document['key'])
                field_name = list(key.keys())[0]
                direction = key[field_name]
                index_names.append(f"{field_name}_{direction}")

            # Create indexes that don't exist
            indexes_to_create = []
            for index in indexes:
                key = dict(index.document['key'])
                if not any(db_indexes.get(name, {}).get('key') == key for name in db_indexes):
                    indexes_to_create.append(index)

            # Delete indexes that are not in the model config
            indexes_to_delete = [
                name for name in db_indexes
                if name not in index_names and name != '_id_'
            ]

            # Create new indexes
            if indexes_to_create:
                try:
                    cls.Q.create_indexes(indexes_to_create)
                except Exception as e:
                    print(f"Error creating indexes: {str(e)}")
                    raise

            # Delete old indexes
            if indexes_to_delete:
                for index_name in indexes_to_delete:
                    try:
                        cls.Q.drop_index(index_name)
                    except Exception as e:
                        print(f"Error dropping index {index_name}: {str(e)}")
                        raise

            # Update the class's __indexes__ attribute
            cls.__indexes__ = set(index_names)

            # Verify indexes were created
            final_indexes = cls.Q.check_indexes()
            if not final_indexes:
                raise Exception("Failed to create indexes")

    def save(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[ClientSession] = None,
    ) -> Any:
        if self._id is not None:
            data = {'_id': ObjectId(self._id)}
            if updated_fields:
                if not all(field in type(self).model_fields for field in updated_fields):
                    raise MongoValidationError(
                        'invalid field in updated_fields')
            else:
                updated_fields = tuple(type(self).model_fields.keys())
            for field in updated_fields:
                data[f'{field}__set'] = getattr(self, field)
            self.Q.update_one(
                session=session,
                **data,
            )
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in type(self).model_fields
        }
        object_id = self.Q.insert_one(
            session=session,
            **data,
        )
        self._id = object_id
        return self

    def delete(self, session: Optional[ClientSession] = None) -> None:
        self.Q.delete_one(_id=ObjectId(self._id), session=session)

    def drop(self, session: Optional[ClientSession] = None) -> None:
        return self.delete(session)

    async def delete_async(self, session: Optional[ClientSession] = None) -> None:
        await self.AQ.delete_one(_id=ObjectId(self._id), session=session)

    async def drop_async(self, session: Optional[ClientSession] = None) -> None:
        return await self.delete_async(session)

    async def save_async(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[ClientSession] = None,
    ) -> Any:
        if self._id is not None:
            data = {'_id': ObjectId(self._id)}
            if updated_fields:
                if not all(field in type(self).model_fields for field in updated_fields):
                    raise MongoValidationError(
                        'invalid field in updated_fields')
            else:
                updated_fields = tuple(type(self).model_fields.keys())
            for field in updated_fields:
                data[f'{field}__set'] = getattr(self, field)
            await self.AQ.update_one(
                session=session,
                **data,
            )
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in type(self).model_fields
        }
        object_id = await self.AQ.insert_one(
            session=session,
            **data,
        )
        self._id = object_id
        return self

    def __hash__(self):
        if self.pk is None:
            raise TypeError(
                "MongoModel instances without _id value are unhashable")
        return hash(self.pk)

    def serialize(self, fields: Union[Tuple, List]) -> 'DictStrAny':
        data: dict = self.model_dump(include=set(fields))
        return {f: data[f] for f in fields}

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return json.dumps(self.serialize(fields))

    @property
    def pk(self):
        return self._id

    def __getattr__(self, prop):
        if prop == '__fields_set__':
            return self.model_fields_set
        if prop == '_id':
            return None
        try:
            return object.__getattribute__(self, prop)
        except AttributeError:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{prop}'")


_is_mongo_model_class_defined = True
