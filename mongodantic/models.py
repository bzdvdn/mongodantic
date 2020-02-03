from typing import TYPE_CHECKING, Dict, Any, Set, List, Generator, Union, Optional
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from bson import ObjectId
from pydantic.main import ModelMetaclass
from pydantic import BaseModel

from .mixins import DBMixin
from .types import ObjectIdStr
from .exceptions import NotDeclaredField, InvalidArgument, ValidationError, MongoIndexError
from .helpers import ExtraQueryMapper, chunk_by_length, bulk_query_generator
from .queryset import QuerySet

SetStr = Set[str]

__all__ = ('MongoModel', 'QuerySet')


class MongoModel(DBMixin, BaseModel):
    _id: ObjectIdStr = None

    def __setattr__(self, key, value):
        if key != '_id':
            return super().__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def set_collection_name(cls) -> str:
        return cls.__name__.lower()


    @classmethod
    def __get_collection(cls) -> Collection:
        return cls._Meta._database.get_collection(cls.set_collection_name())

    @classmethod
    def parse_obj(cls, data: Any) -> Any:
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = str(data['_id'])
        return obj

    @classmethod
    def __validate_query_data(cls, query: Dict, value_validation: bool = False) -> Dict:
        data = {}
        for field, value in query.items():
            field, *extra_params = field.split("__")
            if field not in cls.__fields__ and field != '_id':
                raise NotDeclaredField(field, list(cls.__fields__.keys()))
            _dict = ExtraQueryMapper(field).extra_query(extra_params, value)
            if _dict:
                value = _dict[field]
            elif field == '_id':
                value = ObjectId(value)
            data[field] = value if not value_validation else cls.__validate_value(field, value)
        return data

    @classmethod
    def __validate_value(cls, field_name: str, value: Any) -> Any:
        field = cls.__fields__[field_name]
        value, error_ = field.validate(value, {}, loc=field.alias, cls=cls)
        if error_:
            raise ValidationError([error_], type(cls))
        return value

    @classmethod
    def __query(cls, method_name: str, query_params: Union[List, Dict, str], set_values: Optional[Dict] = None,
                **kwargs) -> Any:
        if isinstance(query_params, dict):
            query_params = cls.__validate_query_data(query_params)
        collection = cls.__get_collection()
        method = getattr(collection, method_name)
        query = (query_params, )
        if set_values:
            query = (query_params, set_values)
        if kwargs:
            return method(*query, **kwargs)
        return method(*query)

    @classmethod
    def check_indexes(cls) -> Dict:
        index_list = list(cls.__query('list_indexes', {}))
        return_dict = {}
        for index in index_list:
            d = dict(index)
            _dict = {'name': d['name'], 'key': dict(d['key'])}
            return_dict.update(_dict)
        return return_dict

    @classmethod
    def add_index(cls, index_name: str, index_type: int, background: bool = True, unique: bool = False,
                  sparse: bool = False) -> str:
        indexes = cls.check_indexes()
        if index_name in indexes:
            raise MongoIndexError(f'{index_name} - already exists.')
        try:
            cls.__query('create_index', [(index_name, index_type)],
                        background=background, unique=unique, sparse=sparse)
            return f'index with name - {index_name} created.'
        except Exception as e:
            raise MongoIndexError(f'detail: {str(e)}')

    @classmethod
    def drop_index(cls, index_name: str) -> str:
        cls.__query('drop_index', index_name)
        return f'{index_name} dropped.'

    @classmethod
    def count(cls, **query) -> int:
        return cls.__query('count', query)

    @classmethod
    def find_one(cls, **query) -> Any:
        data = cls.__query('find_one', query)
        if data:
            obj = cls.parse_obj(data)
            return obj
        return None

    @classmethod
    def find(cls, **query) -> QuerySet:
        data = cls.__query('find', query)
        return QuerySet((cls.parse_obj(obj) for obj in data))

    @classmethod
    def find_with_limit(cls, limit: int = 100, **query):
        data = cls.__query('find', query).limit(limit)
        return QuerySet((cls.parse_obj(obj) for obj in data))

    @classmethod
    def insert_one(cls, **query) -> ObjectId:
        obj = cls.parse_obj(query)
        data = cls.__query('insert_one', obj.data)
        return data.inserted_id

    @classmethod
    def insert_many(cls, data: List) -> int:
        query = []
        for obj in data:
            if not isinstance(obj, ModelMetaclass):
                obj = cls.parse_obj(obj)
            query.append(obj.data)
        r = cls.__query('insert_many', query)
        return len(r.inserted_ids)

    @classmethod
    def delete_one(cls, **query) -> int:
        r = cls.__query('delete_one', query)
        return r.deleted_count

    @classmethod
    def delete_many(cls, **query) -> int:
        r = cls.__query('delete_many', query)
        return r.deleted_count

    @classmethod
    def _ensure_update_data(cls, **fields) -> tuple:
        if not any("__set" in f for f in fields):
            raise AttributeError("not fields for updating!")
        queries = {}
        set_values = {}
        for name, value in fields.items():
            extra_fields = name.split("__")
            if len(extra_fields) == 2:
                if extra_fields[1] == "set":
                    _dict = cls.__validate_query_data({extra_fields[0]: value}, value_validation=True)
                    set_values.update(_dict)
            else:
                _dict = cls.__validate_query_data({name: value})
                queries.update(_dict)
        return queries, set_values

    @classmethod
    def replace_one(cls, replacement: Dict, upsert: bool = False, **filter_query) -> Any:
        if not filter_query:
            raise AttributeError('not filter parameters')
        if not replacement:
            raise AttributeError('not replacement parameters')
        filter_query = cls.__validate_query_data(filter_query)
        replacement = cls.__validate_query_data(replacement)
        return cls.__query('replace_one', filter_query, replacement=replacement, upsert=upsert)

    @classmethod
    def raw_query(cls, method_name: str, raw_query: Union[Dict, List[Dict]]) -> Any:
        if 'insert' in method_name or 'replace' in method_name or 'update' in method_name:
            if isinstance(raw_query, list):
                raw_query = list(map(cls.__validate_query_data, raw_query))
            else:
                raw_query = cls.__validate_query_data(raw_query)
        collection = cls.__get_collection()
        query = getattr(collection, method_name)
        return query(raw_query)

    @classmethod
    def _update(cls, method: str, query: Dict) -> int:
        query, set_values = cls._ensure_update_data(**query)
        r = cls.__query(method, query, {'$set': set_values})
        return r.modified_count

    @classmethod
    def update_one(cls, **query) -> int:
        return cls._update('update_one', query)

    @classmethod
    def update_many(cls, **query) -> int:
        return cls._update('update_many', query)

    @classmethod
    def _aggregate(cls, operation: str, agg_field: str, **query) -> int:
        query = cls.__validate_query_data(query)
        data = [
            {"$match": query},
            {"$group": {"_id": None, "total": {f"${operation}": f"${agg_field}"}}},
        ]
        try:
            return cls.__query("aggregate", data).next()["total"]
        except StopIteration:
            return 0

    @classmethod
    def _bulk_operation(cls, models: List, updated_fields: Optional[List] = None,
                        query_fields: Optional[List] = None, batch_size: Optional[int] = None,
                        upsert: bool = False) -> None:
        if batch_size is not None and batch_size > 0:
            for requests in chunk_by_length(models, batch_size):
                data = bulk_query_generator(requests, updated_fields=updated_fields, query_fields=query_fields,
                                            upsert=upsert)
                cls.__query('bulk_write', data)
            return None
        data = bulk_query_generator(models, updated_fields=updated_fields, query_fields=query_fields, upsert=upsert)
        cls.__query('bulk_write', data)

    @classmethod
    def bulk_update(cls, models: List, updated_fields: List, batch_size: Optional[int] = None) -> None:
        if not updated_fields:
            raise ValidationError('updated_fields cannot be empty')
        return cls._bulk_operation(models, updated_fields=updated_fields, batch_size=batch_size)

    @classmethod
    def bulk_update_or_create(cls, models: List, query_fields: List, batch_size: Optional[int] = None) -> None:
        if not query_fields:
            raise ValidationError('query_fields cannot be empty')
        return cls._bulk_operation(models, query_fields=query_fields, batch_size=batch_size, upsert=True)

    @classmethod
    def aggregate_sum(cls, agg_field: str, **query) -> int:
        return cls._aggregate('sum', agg_field, **query)

    @classmethod
    def aggregate_max(cls, agg_field: str, **query) -> int:
        return cls._aggregate('max', agg_field, **query)

    @classmethod
    def aggregate_min(cls, agg_field: str, **query) -> int:
        return cls._aggregate('min', agg_field, **query)

    @property
    def data(self) -> Dict:
        return self.dict()

    def save(self) -> Any:
        if self._id is not None:
            data = {'_id': ObjectId(self._id)}
            for field in self.__fields__:
                data[f'{field}__set'] = getattr(self, field)
            self.update_one(**data)
            return self
        data = {field: value for field, value in self.__dict__.items() if field in self.__fields__}
        object_id = self.insert_one(**data)
        self._id = str(object_id)
        return self
