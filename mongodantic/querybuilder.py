from typing import Union, List, Dict, Optional, Any
from pymongo.collection import Collection
from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.errors import (
    BulkWriteError,
    NetworkTimeout,
    AutoReconnect,
    ConnectionFailure,
    WriteConcernError,
    ServerSelectionTimeoutError,
)
from bson import ObjectId
from pydantic.main import ModelMetaclass
from pydantic import BaseModel as BasePydanticModel

from .exceptions import (
    ValidationError,
    MongoIndexError,
    MongoConnectionError,
)
from .helpers import (
    chunk_by_length,
    bulk_query_generator,
    generate_lookup_project_params,
    generate_operator_for_multiply_aggregations,
)
from .queryset import QuerySet
from .logical import LogicalCombination, Query


class MongoQueryBuilderMixin(object):
    @classmethod
    def _query(
        cls,
        method_name: str,
        query_params: Union[List, Dict, str, Query, LogicalCombination],
        set_values: Optional[Dict] = None,
        session: Optional[ClientSession] = None,
        counter: int = 1,
        logical: bool = False,
        **kwargs,
    ) -> Any:
        inner_query_params = query_params
        if logical:
            query_params = cls._check_query_args(query_params)
        elif isinstance(query_params, dict):
            query_params = cls._validate_query_data(query_params)

        method = getattr(cls.collection, method_name)
        query = (query_params,)
        if session:
            kwargs['session'] = session
        if set_values:
            query = (query_params, set_values)
        try:
            if kwargs:
                return method(*query, **kwargs)
            return method(*query)
        except (
            NetworkTimeout,
            AutoReconnect,
            ConnectionFailure,
            WriteConcernError,
            ServerSelectionTimeoutError,
        ) as description:
            cls._reconnect()
            if counter >= 5:
                raise MongoConnectionError(str(description))
            counter += 1
            return cls._query(
                method_name=method_name,
                query_params=inner_query_params,
                set_values=set_values,
                session=session,
                counter=counter,
                logical=logical,
                **kwargs,
            )

    @classmethod
    def check_indexes(cls) -> List:
        index_list = list(cls._query('list_indexes', {}))
        return_list = []
        for index in index_list:
            d = dict(index)
            _dict = {'name': d['name'], 'key': dict(d['key'])}
            return_list.append(_dict)
        return return_list

    @classmethod
    def add_index(
        cls,
        index_name: str,
        index_type: int,
        background: bool = True,
        unique: bool = False,
        sparse: bool = False,
    ) -> str:
        indexes = [index['name'] for index in cls.check_indexes()]
        if f'{index_name}_{index_type}' in indexes:
            raise MongoIndexError(f'{index_name} - already exists.')
        try:
            cls._query(
                'create_index',
                [(index_name, index_type)],
                background=background,
                unique=unique,
                sparse=sparse,
            )
            return f'index with name - {index_name} created.'
        except Exception as e:
            raise MongoIndexError(f'detail: {str(e)}')

    @classmethod
    def drop_index(cls, index_name: str) -> str:
        indexes = cls.check_indexes()
        drop = False
        for index in indexes:
            if f'{index_name}_' in index['name']:
                drop = True
                cls._query('drop_index', index['name'])
        if drop:
            return f'{index_name} dropped.'
        raise MongoIndexError(f'invalid index name - {index_name}')

    @classmethod
    def count(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        return cls._query(
            'count',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )

    @classmethod
    def find_one(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        **query,
    ) -> Any:
        sort_values = [(field, sort) for field in sort_fields]
        data = cls._query(
            'find_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
            sort=sort_values,
        )
        if data:
            obj = cls.parse_obj(data)
            return obj
        return None

    @classmethod
    def find(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        **query,
    ) -> QuerySet:
        data = cls._query(
            'find', logical_query or query, session=session, logical=bool(logical_query)
        )
        if skip_rows is not None:
            data = data.skip(skip_rows)
        if limit_rows:
            data = data.limit(limit_rows)
        if sort not in (1, -1):
            raise ValueError(f'invalid sort value must be 1 or -1 not {sort}')
        sort_values = [(field, sort) for field in sort_fields]

        return QuerySet(cls, data.sort(sort_values))

    @classmethod
    def find_with_count(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        **query,
    ) -> tuple:

        count = cls.count(**query, session=session, logical_query=logical_query)
        results = cls.find(
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            session=session,
            logical_query=logical_query,
            sort_fields=sort_fields,
            sort=sort,
            **query,
        )
        return count, results

    @classmethod
    def insert_one(cls, session: Optional[ClientSession] = None, **query) -> ObjectId:
        obj = cls.parse_obj(query)
        data = cls._query('insert_one', obj.data, session=session)
        return data.inserted_id

    @classmethod
    def insert_many(cls, data: List, session: Optional[ClientSession] = None) -> int:
        parse_obj = cls.parse_obj
        query = [
            parse_obj(obj).data if isinstance(obj, ModelMetaclass) else obj.data
            for obj in data
        ]
        r = cls._query('insert_many', query, session=session)
        return len(r.inserted_ids)

    @classmethod
    def delete_one(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:

        r = cls._query(
            'delete_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    @classmethod
    def delete_many(
        cls,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        *args,
        **query,
    ) -> int:

        r = cls._query(
            'delete_many',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    @classmethod
    def _ensure_update_data(cls, **fields) -> tuple:
        if not any("__set" in f for f in fields):
            raise ValueError("not fields for updating!")
        queries = {}
        set_values = {}
        for name, value in fields.items():
            if name.endswith('__set'):
                name = name.replace('__set', '')
                data = cls._validate_query_data({name: value})
                set_values.update(data)
            else:
                data = cls._validate_query_data({name: value})
                queries.update(data)
        return queries, set_values

    @classmethod
    def replace_one(
        cls,
        replacement: Dict,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **filter_query,
    ) -> Any:
        if not filter_query:
            raise AttributeError('not filter parameters')
        if not replacement:
            raise AttributeError('not replacement parameters')
        return cls._query(
            'replace_one',
            cls._validate_query_data(filter_query),
            replacement=cls._validate_query_data(replacement),
            upsert=upsert,
            session=session,
        )

    @classmethod
    def raw_query(
        cls,
        method_name: str,
        raw_query: Union[Dict, List[Dict]],
        session: Optional[ClientSession] = None,
    ) -> Any:
        if (
            'insert' in method_name
            or 'replace' in method_name
            or 'update' in method_name
        ):
            if isinstance(raw_query, list):
                raw_query = list(map(cls._validate_query_data, raw_query))
            else:
                raw_query = cls._validate_query_data(raw_query)
        collection = cls._get_collection()
        query = getattr(collection, method_name)
        return query(raw_query, session=session)

    @classmethod
    def _update(
        cls,
        method: str,
        query: Dict,
        upsert: bool = True,
        session: Optional[ClientSession] = None,
    ) -> int:
        query, set_values = cls._ensure_update_data(**query)
        r = cls._query(
            method, query, {'$set': set_values}, upsert=upsert, session=session
        )
        return r.modified_count

    @classmethod
    def update_one(
        cls, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        return cls._update('update_one', query, upsert=upsert, session=session)

    @classmethod
    def update_many(
        cls, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        return cls._update('update_many', query, upsert=upsert, session=session)

    @classmethod
    def aggregate_count(
        cls, agg_field: str, session: Optional[ClientSession] = None, **query,
    ) -> dict:
        data = [
            {"$match": cls._validate_query_data(query)},
            {"$group": {"_id": f'${agg_field}', "count": {"$sum": 1}}},
        ]
        result = cls._query("aggregate", data, session=session)
        return {r['_id']: r['count'] for r in result}

    @classmethod
    def aggregate_multiply_count(
        cls,
        agg_fields: Union[List, tuple],
        session: Optional[ClientSession] = None,
        **query,
    ) -> list:
        data = [
            {"$match": cls._validate_query_data(query)},
            {
                "$group": {
                    "_id": {field: f'${field}' for field in agg_fields},
                    "count": {"$sum": 1},
                }
            },
        ]

        result = cls._query("aggregate", data, session=session)
        return list(result)

    @classmethod
    def aggregate_multiply_math_operations(
        cls,
        agg_fields: Union[list, tuple],
        fields_operations: dict,
        session: Optional[ClientSession] = None,
        **query,
    ):
        for f in agg_fields:
            if f not in fields_operations:
                raise ValidationError(f'{f} not in fields_operations')
            elif fields_operations[f] not in ('sum', 'max', 'min'):
                raise ValidationError(
                    f'{fields_operations[f]} invalid aggregation operation'
                )
        return cls._aggregate_multiply_math_operations(
            agg_fields=agg_fields,
            fields_operations=fields_operations,
            session=session,
            **query,
        )

    @classmethod
    def _aggregate_multiply_math_operations(
        cls,
        agg_fields: Union[tuple, list],
        operation: Optional[str] = None,
        session: Optional[ClientSession] = None,
        fields_operations: Optional[dict] = None,
        **query,
    ) -> dict:
        if not operation and not fields_operations:
            raise ValidationError('miss operation or fields_operations')

        aggregate_query = {
            f'{f}__{generate_operator_for_multiply_aggregations(f, operation, fields_operations)}': {
                f"${generate_operator_for_multiply_aggregations(f, operation, fields_operations)}": f"${f}"
            }
            for f in agg_fields
        }
        data = [
            {"$match": cls._validate_query_data(query)},
            {"$group": {"_id": None, **aggregate_query}},
        ]
        try:
            result = cls._query("aggregate", data, session=session).next()
            return {f: result[f] for f in result if f.split('__')[0] in agg_fields}
        except StopIteration:
            return {
                f'{f}__{generate_operator_for_multiply_aggregations(f, operation, fields_operations)}': 0
                for f in agg_fields
            }

    @classmethod
    def aggregate_sum_multiply(
        cls,
        agg_fields: Union[tuple, list],
        session: Optional[ClientSession] = None,
        **query,
    ):
        return cls._aggregate_multiply_math_operations(
            operation='sum', agg_fields=agg_fields, session=session, **query
        )

    @classmethod
    def aggregate_max_multiply(
        cls,
        agg_fields: Union[tuple, list],
        session: Optional[ClientSession] = None,
        **query,
    ):
        return cls._aggregate_multiply_math_operations(
            operation='max', agg_fields=agg_fields, session=session, **query
        )

    @classmethod
    def aggregate_min_multiply(
        cls,
        agg_fields: Union[tuple, list],
        session: Optional[ClientSession] = None,
        **query,
    ):
        return cls._aggregate_multiply_math_operations(
            operation='min', agg_fields=agg_fields, session=session, **query
        )

    @classmethod
    def _aggregate_math_operation(
        cls,
        operation: str,
        agg_field: str,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Union[int, QuerySet]:
        data = [
            {"$match": cls._validate_query_data(query)},
            {"$group": {"_id": None, "total": {f"${operation}": f"${agg_field}"}}},
        ]
        try:
            return cls._query("aggregate", data, session=session).next()["total"]
        except StopIteration:
            return 0

    @classmethod
    def aggregate_lookup(
        cls,
        local_field: str,
        from_collection: ModelMetaclass,
        foreign_field: Optional[str] = None,
        as_: Optional[str] = None,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        with_unwing: bool = False,
        **query,
    ) -> QuerySet:
        lookup = {
            "$lookup": {
                "localField": local_field,
                "from": from_collection.__name__.lower(),
                "foreignField": foreign_field if foreign_field else "_id",
                "as": as_ if as_ else from_collection.__name__.lower(),
            }
        }
        project_param = generate_lookup_project_params(
            cls, from_collection, lookup["$lookup"]["as"]
        )
        query_params = [
            {
                '$match': cls._check_query_args(logical_query)
                if logical_query
                else cls._validate_query_data(query)
            },
            lookup,
            {'$project': project_param},
            {'$sort': {sf: sort for sf in sort_fields}},
        ]
        if with_unwing:
            query_params.append({"$unwind": f'${lookup["$lookup"]["as"]}'})
        if limit_rows:
            query_params.append({'$limit': limit_rows})
        data = cls._query(
            "aggregate", query_params, session=session, logical=bool(logical_query)
        )
        if skip_rows:
            data = data.skip(skip_rows)
        return QuerySet(cls, data, reference_model=from_collection)

    @classmethod
    def _bulk_operation(
        cls,
        models: List,
        updated_fields: Optional[List] = None,
        query_fields: Optional[List] = None,
        batch_size: Optional[int] = 10000,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
    ) -> None:
        if batch_size is not None and batch_size > 0:
            for requests in chunk_by_length(models, batch_size):
                data = bulk_query_generator(
                    requests,
                    updated_fields=updated_fields,
                    query_fields=query_fields,
                    upsert=upsert,
                )
                cls._query('bulk_write', data, session=session)
            return None
        data = bulk_query_generator(
            models,
            updated_fields=updated_fields,
            query_fields=query_fields,
            upsert=upsert,
        )
        cls._query('bulk_write', data, session=session)

    @classmethod
    def bulk_update(
        cls,
        models: List,
        updated_fields: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not updated_fields:
            raise ValidationError('updated_fields cannot be empty')
        return cls._bulk_operation(
            models,
            updated_fields=updated_fields,
            batch_size=batch_size,
            session=session,
        )

    @classmethod
    def bulk_update_or_create(
        cls,
        models: List,
        query_fields: List,
        batch_size: Optional[int] = 10000,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not query_fields:
            raise ValidationError('query_fields cannot be empty')
        return cls._bulk_operation(
            models,
            query_fields=query_fields,
            batch_size=batch_size,
            upsert=True,
            session=session,
        )

    @classmethod
    def aggregate_sum(
        cls, agg_field: str, session: Optional[ClientSession] = None, **query
    ) -> int:
        return cls._aggregate_math_operation('sum', agg_field, session=session, **query)

    @classmethod
    def aggregate_max(
        cls, agg_field: str, session: Optional[ClientSession] = None, **query
    ) -> int:
        return cls._aggregate_math_operation('max', agg_field, session=session, **query)

    @classmethod
    def aggregate_min(
        cls, agg_field: str, session: Optional[ClientSession] = None, **query
    ) -> int:
        return cls._aggregate_math_operation('min', agg_field, session=session, **query)

    @property
    def data(self) -> Dict:
        return self.dict()

    def serialize(self, fields: Union[tuple, list]) -> dict:
        data = self.dict(include=set(fields))
        return {f: data[f] for f in fields}

    @classmethod
    def _find_with_replacement_or_with_update(
        cls,
        operation: str,
        projection_fields: Optional[list] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        filter_, set_values = cls._ensure_update_data(**query)
        return_document = ReturnDocument.AFTER
        sort = [(field, sort) for field in sort_fields]
        replacement = query.pop('replacement', None)

        projection = {f: True for f in projection_fields} if projection_fields else None
        extra_params = {
            'return_document': return_document,
            'projection': projection,
            'upsert': upsert,
            'sort': sort,
            'session': session,
        }

        if replacement:
            extra_params['replacement'] = replacement

        data = cls._query(operation, filter_, {'$set': set_values}, **extra_params)
        if projection:
            return {
                field: value for field, value in data.items() if field in projection
            }
        return cls.parse_obj(data)

    @classmethod
    def find_one_and_update(
        cls,
        projection_fields: Optional[list] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ):

        return cls._find_with_replacement_or_with_update(
            'find_one_and_update',
            projection_fields=projection_fields,
            sort_fields=sort_fields,
            sort=sort,
            upsert=upsert,
            session=session,
            **query,
        )

    @classmethod
    def find_and_replace(
        cls,
        replacement: Union[dict, Any],
        projection_fields: Optional[list] = None,
        sort_fields: Union[tuple, list] = ('_id',),
        sort: int = 1,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        if isinstance(replacement, BasePydanticModel):
            replacement = replacement.data
        return cls._find_with_replacement_or_with_update(
            'find_and_replace',
            projection_fields=projection_fields,
            sort_fields=sort_fields,
            sort=sort,
            upsert=upsert,
            session=session,
            replacement=replacement,
            **query,
        )

    @classmethod
    def drop_collection(cls, force: bool = False) -> str:
        if force:
            cls._query('drop', query_params={})
            return f'{cls.__name__.lower()} - dropped!'
        value = input(
            f'Are u sure for drop this collection - {cls.__name__.lower()} (y, n)'
        )
        if value.lower() == 'y':
            cls._query('drop', query_params={})
            return f'{cls.__name__.lower()} - dropped!'
        return 'nope'
