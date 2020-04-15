from typing import TYPE_CHECKING, Dict, Any, Set, List, Generator, Union, Optional
from pymongo.collection import Collection
from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.errors import BulkWriteError, NetworkTimeout, AutoReconnect, ConnectionFailure, WriteConcernError, \
    ServerSelectionTimeoutError
from bson import ObjectId
from pydantic.main import ModelMetaclass
from pydantic import BaseModel

from .mixins import DBMixin
from .types import ObjectIdStr
from .exceptions import NotDeclaredField, InvalidArgument, ValidationError, MongoIndexError, MongoConnectionError, \
    InvalidArgsParams
from .helpers import ExtraQueryMapper, chunk_by_length, bulk_query_generator, generate_lookup_project_params
from .queryset import QuerySet
from .logical import LogicalCombination, Query

SetStr = Set[str]

__all__ = ('MongoModel', 'QuerySet', 'Query')


class MongoModel(DBMixin, BaseModel):
    _id: ObjectIdStr = None
    __reference__ = None

    def __setattr__(self, key, value):
        if key in self.__fields__:
            return super().__setattr__(key, value)
        self.__dict__[key] = value
        return value

    @classmethod
    def set_collection_name(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def _get_collection(cls) -> Collection:
        return cls._Meta._database.get_collection(cls.set_collection_name())

    @classmethod
    def parse_obj(cls, data: Any, reference_model: Optional[ModelMetaclass] = None) -> Any:
        obj = super().parse_obj(data)
        if '_id' in data:
            obj._id = data['_id']
        if reference_model:
            obj = cls.__set_reference_fields(obj, data, reference_model)
        return obj

    @classmethod
    def __set_reference_fields(cls, obj: ModelMetaclass, data: Dict, ref: ModelMetaclass) -> ModelMetaclass:
        data = data[ref.__name__.lower()]
        if isinstance(data, dict):
            ref_obj = ref.parse_obj(data)
        else:
            ref_obj = [ref.parse_obj(d) for d in data]
        setattr(obj, f'{ref.__name__.lower()}', ref_obj)
        return obj

    @classmethod
    def _validate_query_data(cls, query: Dict) -> Dict:
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
            else:
                value = cls.__validate_value(field, value)
            data[field] = value
        return data

    @classmethod
    def __validate_value(cls, field_name: str, value: Any) -> Any:
        field = cls.__fields__[field_name]
        if isinstance(field, ObjectIdStr):
            value, error_ = field.validate(value)
        else:
            value, error_ = field.validate(value, {}, loc=field.alias, cls=cls)
        if error_:
            raise ValidationError([error_], type(value))
        return value

    @classmethod
    def __check_query_args(cls, logical: Union[Query, LogicalCombination, None] = None) -> Dict:
        if not isinstance(logical, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical.to_query(cls)

    @classmethod
    def __query(cls, method_name: str, query_params: Union[List, Dict, str], set_values: Optional[Dict] = None,
                session: Optional[ClientSession] = None, counter: int = 1, logical: bool = False, **kwargs) -> Any:
        inner_query_params = query_params
        if isinstance(query_params, dict) and not logical:
            query_params = cls._validate_query_data(query_params)
        collection = cls._get_collection()
        method = getattr(collection, method_name)
        query = (query_params,)
        if session:
            kwargs['session'] = session
        if set_values:
            query = (query_params, set_values)
        try:
            if kwargs:
                return method(*query, **kwargs)
            return method(*query)
        except (NetworkTimeout, AutoReconnect, ConnectionFailure, WriteConcernError,
                ServerSelectionTimeoutError) as description:
            cls._reconnect()
            if counter >= 5:
                raise MongoConnectionError(str(description))
            counter += 1
            return cls.__query(method_name=method_name, query_params=inner_query_params, logical=logical,
                               set_values=set_values, session=session, counter=counter, **kwargs)

    @classmethod
    def check_indexes(cls) -> List:
        index_list = list(cls.__query('list_indexes', {}))
        return_list = []
        for index in index_list:
            d = dict(index)
            _dict = {'name': d['name'], 'key': dict(d['key'])}
            return_list.append(_dict)
        return return_list

    @classmethod
    def add_index(cls, index_name: str, index_type: int, background: bool = True, unique: bool = False,
                  sparse: bool = False) -> str:
        indexes = [index['name'] for index in cls.check_indexes()]
        if f'{index_name}_{index_type}' in indexes:
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
    def count(cls, logical: Union[Query, LogicalCombination, None] = None,
              session: Optional[ClientSession] = None, **query) -> int:
        if logical:
            query = cls.__check_query_args(logical)
        return cls.__query('count', query, session=session, logical=bool(logical))

    @classmethod
    def find_one(cls, logical: Union[Query, LogicalCombination, None] = None,
                 session: Optional[ClientSession] = None,
                 sort_fields: Union[tuple, list] = ('_id',),
                 sort: int = 1,
                 **query) -> Any:
        if logical:
            query = cls.__check_query_args(logical)
        sort_values = [(field, sort) for field in sort_fields]
        data = cls.__query('find_one', query, session=session, logical=bool(logical), sort=sort_values)
        if data:
            obj = cls.parse_obj(data)
            return obj
        return None

    @classmethod
    def find(cls, logical: Union[Query, LogicalCombination, None] = None, skip_rows: Optional[int] = None,
             limit_rows: Optional[int] = None,
             session: Optional[ClientSession] = None,
             sort_fields: Union[tuple, list] = ('_id',),
             sort: int = 1,
             **query) -> QuerySet:
        if logical:
            query = cls.__check_query_args(logical)
        data = cls.__query('find', query, session=session, logical=bool(logical))
        if skip_rows is not None:
            data = data.skip(skip_rows)
        if limit_rows:
            data = data.limit(limit_rows)
        if sort not in (1, -1):
            raise ValueError(f'invalid sort value must be 1 or -1 not {sort}')
        sort_values = [(field, sort) for field in sort_fields]

        return QuerySet(cls, data.sort(sort_values))

    @classmethod
    def find_with_count(cls, logical: Union[Query, LogicalCombination, None] = None, skip_rows: Optional[int] = None,
                        limit_rows: Optional[int] = None,
                        session: Optional[ClientSession] = None,
                        sort_fields: Union[tuple, list] = ('_id',),
                        sort: int = 1,
                        **query) -> tuple:
        if logical:
            query = cls.__check_query_args(logical)
        count = cls.count(**query, session=session, logical=logical)
        results = cls.find(skip_rows=skip_rows, limit_rows=limit_rows, session=session, logical=logical,
                           sort_fields=sort_fields, sort=sort, **query)
        return count, results

    @classmethod
    def insert_one(cls, session: Optional[ClientSession] = None, **query) -> ObjectId:
        obj = cls.parse_obj(query)
        data = cls.__query('insert_one', obj.data, session=session)
        return data.inserted_id

    @classmethod
    def insert_many(cls, data: List, session: Optional[ClientSession] = None) -> int:
        query = []
        for obj in data:
            if not isinstance(obj, ModelMetaclass):
                obj = cls.parse_obj(obj)
            query.append(obj.data)
        r = cls.__query('insert_many', query, session=session)
        return len(r.inserted_ids)

    @classmethod
    def delete_one(cls, logical: Union[Query, LogicalCombination, None] = None, session: Optional[ClientSession] = None,
                   **query) -> int:
        if logical:
            query = cls.__check_query_args(logical)
        r = cls.__query('delete_one', query, session=session, logical=bool(logical))
        return r.deleted_count

    @classmethod
    def delete_many(cls, logical: Union[Query, LogicalCombination, None] = None,
                    session: Optional[ClientSession] = None, *args, **query) -> int:
        if logical:
            query = cls.__check_query_args(logical)
        r = cls.__query('delete_many', query, session=session, logical=bool(logical))
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
                    _dict = cls._validate_query_data({extra_fields[0]: value})
                    set_values.update(_dict)
            else:
                _dict = cls._validate_query_data({name: value})
                queries.update(_dict)
        return queries, set_values

    @classmethod
    def replace_one(cls, replacement: Dict, upsert: bool = False,
                    session: Optional[ClientSession] = None,
                    **filter_query) -> Any:
        if not filter_query:
            raise AttributeError('not filter parameters')
        if not replacement:
            raise AttributeError('not replacement parameters')
        filter_query = cls._validate_query_data(filter_query)
        replacement = cls._validate_query_data(replacement)
        return cls.__query('replace_one', filter_query, replacement=replacement, upsert=upsert, session=session)

    @classmethod
    def raw_query(cls, method_name: str, raw_query: Union[Dict, List[Dict]],
                  session: Optional[ClientSession] = None) -> Any:
        if 'insert' in method_name or 'replace' in method_name or 'update' in method_name:
            if isinstance(raw_query, list):
                raw_query = list(map(cls._validate_query_data, raw_query))
            else:
                raw_query = cls._validate_query_data(raw_query)
        collection = cls._get_collection()
        query = getattr(collection, method_name)
        return query(raw_query, session=session)

    @classmethod
    def _update(cls, method: str, query: Dict, upsert: bool = True, session: Optional[ClientSession] = None) -> int:
        query, set_values = cls._ensure_update_data(**query)
        r = cls.__query(method, query, {'$set': set_values}, upsert=upsert, session=session)
        return r.modified_count

    @classmethod
    def update_one(cls, upsert: bool = False, session: Optional[ClientSession] = None, **query) -> int:
        return cls._update('update_one', query, upsert=upsert, session=session)

    @classmethod
    def update_many(cls, upsert: bool = False, session: Optional[ClientSession] = None, **query) -> int:
        return cls._update('update_many', query, upsert=upsert, session=session)

    @classmethod
    def _aggregate_math_operation(cls, operation: str, agg_field: str, session: Optional[ClientSession] = None,
                                  **query) -> Union[int, QuerySet]:
        query = cls._validate_query_data(query)
        data = [
            {"$match": query},
            {"$group": {"_id": None, "total": {f"${operation}": f"${agg_field}"}}},
        ]
        try:
            return cls.__query("aggregate", data, session=session).next()["total"]
        except StopIteration:
            return 0

    @classmethod
    def aggregate_lookup(cls, local_field: str, from_collection: ModelMetaclass,
                         foreign_field: Optional[str] = None,
                         as_: Optional[str] = None,
                         logical: Union[Query, LogicalCombination, None] = None, skip_rows: Optional[int] = None,
                         limit_rows: Optional[int] = None,
                         session: Optional[ClientSession] = None,
                         sort_fields: Union[tuple, list] = ('_id',),
                         sort: int = 1,
                         with_unwing: bool = False,
                         **query) -> QuerySet:
        if logical:
            query = cls.__check_query_args(logical)
        else:
            query = cls._validate_query_data(query)
        lookup = {
            "$lookup": {
                "localField": local_field,
                "from": from_collection.__name__.lower(),
                "foreignField": foreign_field if foreign_field else "_id",
                "as": as_ if as_ else from_collection.__name__.lower()
            }
        }
        project_param = generate_lookup_project_params(cls, from_collection, lookup["$lookup"]["as"])
        query_params = [
            {'$match': query},
            lookup,
            {'$project': project_param},
            {'$sort': {sf: sort for sf in sort_fields}}
        ]
        if with_unwing:
            query_params.append({"$unwind": f'${lookup["$lookup"]["as"]}'})
        if limit_rows:
            query_params.append({'$limit': limit_rows})
        data = cls.__query("aggregate", query_params, session=session, logical=bool(logical))
        if skip_rows:
            data = data.skip(skip_rows)
        return QuerySet(cls, data, reference_model=from_collection)

    @classmethod
    def _bulk_operation(cls, models: List, updated_fields: Optional[List] = None,
                        query_fields: Optional[List] = None, batch_size: Optional[int] = None,
                        upsert: bool = False, session: Optional[ClientSession] = None) -> None:
        if batch_size is not None and batch_size > 0:
            for requests in chunk_by_length(models, batch_size):
                data = bulk_query_generator(requests, updated_fields=updated_fields, query_fields=query_fields,
                                            upsert=upsert)
                cls.__query('bulk_write', data, session=session)
            return None
        data = bulk_query_generator(models, updated_fields=updated_fields, query_fields=query_fields, upsert=upsert)
        cls.__query('bulk_write', data, session=session)

    @classmethod
    def bulk_update(cls, models: List, updated_fields: List, batch_size: Optional[int] = None,
                    session: Optional[ClientSession] = None) -> None:
        if not updated_fields:
            raise ValidationError('updated_fields cannot be empty')
        return cls._bulk_operation(models, updated_fields=updated_fields, batch_size=batch_size, session=session)

    @classmethod
    def bulk_update_or_create(cls, models: List, query_fields: List, batch_size: Optional[int] = None,
                              session: Optional[ClientSession] = None) -> None:
        if not query_fields:
            raise ValidationError('query_fields cannot be empty')
        return cls._bulk_operation(models, query_fields=query_fields, batch_size=batch_size, upsert=True,
                                   session=session)

    @classmethod
    def aggregate_sum(cls, agg_field: str, session: Optional[ClientSession] = None, **query) -> int:
        return cls._aggregate_math_operation('sum', agg_field, session=session, **query)

    @classmethod
    def aggregate_max(cls, agg_field: str, session: Optional[ClientSession] = None, **query) -> int:
        return cls._aggregate_math_operation('max', agg_field, session=session, **query)

    @classmethod
    def aggregate_min(cls, agg_field: str, session: Optional[ClientSession] = None, **query) -> int:
        return cls._aggregate_math_operation('min', agg_field, session=session, **query)

    @property
    def data(self) -> Dict:
        return self.dict()

    @classmethod
    def find_one_and_update(cls, projection: Optional[dict] = None, sort: Optional[dict] = None,
                            upsert: bool = False, session: Optional[ClientSession] = None, **query) -> Any:
        filter_, set_values = cls._ensure_update_data(**query)
        return_document = ReturnDocument.AFTER
        if sort is not None:
            sort = [(key, value) for key, value in sort.items()]
        data = cls.__query(
            'find_one_and_update',
            filter_,
            {'$set': set_values},
            return_document=return_document,
            projection=projection,
            upsert=upsert,
            sort=sort,
            session=session,
        )
        if projection:
            return {field: value for field, value in data.items() if field in projection}
        return cls.parse_obj(data)

    @classmethod
    def find_and_replace(cls, replacement: Union[dict, Any],
                         projection: Optional[dict] = None, sort: Optional[dict] = None,
                         upsert: bool = False, session: Optional[ClientSession] = None,
                         **query) -> Any:
        if isinstance(replacement, BaseModel):
            replacement = replacement.data
        query = cls._validate_query_data(query)
        if sort is not None:
            sort = [(key, value) for key, value in sort.items()]
        data = cls.__query(
            'find_one_and_replace',
            query,
            replacement,
            return_document=ReturnDocument.AFTER,
            projection=projection,
            upsert=upsert,
            sort=sort,
            session=session,
        )
        if projection:
            return {field: value for field, value in data.items() if projection.get(field)}
        return cls.parse_obj(data)

    def save(self, session: Optional[ClientSession] = None) -> Any:
        if self._id is not None:
            data = {'_id': ObjectId(self._id)}
            for field in self.__fields__:
                data[f'{field}__set'] = getattr(self, field)
            self.update_one(**data)
            return self
        data = {field: value for field, value in self.__dict__.items() if field in self.__fields__}
        object_id = self.insert_one(**data, session=session)
        self._id = object_id.__str__()
        return self

    def _start_session(self) -> ClientSession:
        return self._Meta._connection._mongo_connection.start_session()
