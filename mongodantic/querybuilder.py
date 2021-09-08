from typing import Union, List, Dict, Optional, Any, Tuple, TYPE_CHECKING, Generator
from collections.abc import Iterable
from abc import ABC
from pymongo import ReturnDocument
from pymongo import IndexModel
from pymongo.client_session import ClientSession
from bson import ObjectId

from .exceptions import (
    ValidationError,
    MongoIndexError,
)
from .helpers import (
    chunk_by_length,
    bulk_query_generator,
    generate_name_field,
    sort_validation,
    group_by_aggregate_generation,
    handle_and_convert_connection_errors,
)
from .queryset import QuerySet
from .logical import LogicalCombination, Query
from .aggregation import Sum, Max, Min, Avg
from .exceptions import DoesNotExist
from .sync_async import sync_to_async

if TYPE_CHECKING:
    from .models import MongoModel


class BaseQueryBuilder(ABC):
    def __init__(self, mongo_model: 'MongoModel'):
        self._mongo_model = mongo_model

    def add_model(self, mongo_model):
        if not self._mongo_model:
            self._mongo_model = mongo_model

    @handle_and_convert_connection_errors
    def __query(
        self,
        method_name: str,
        query_params: Union[List, Dict, str, Query, LogicalCombination],
        set_values: Optional[Dict] = None,
        session: Optional[ClientSession] = None,
        logical: bool = False,
        **kwargs,
    ) -> Any:
        if logical:
            query_params = self._mongo_model._check_query_args(query_params)
        elif isinstance(query_params, dict):
            query_params = self._mongo_model._validate_query_data(query_params)

        method = getattr(self._mongo_model._collection, method_name)
        query = [query_params]
        if session:
            kwargs['session'] = session
        if set_values:
            query = [query_params, set_values]
        if kwargs:
            return method(*query, **kwargs)
        return method(*query)

    def check_indexes(self) -> dict:
        index_list = list(self.__query('list_indexes', {}))
        return_data = {}
        for index in index_list:
            d = dict(index)
            _dict = {d['name']: {'key': dict(d['key'])}}
            return_data.update(_dict)
        return return_data

    def create_indexes(
        self, indexes: List[IndexModel], session: Optional[ClientSession] = None,
    ) -> List[str]:
        return self.__query('create_indexes', indexes, session=session)

    def drop_index(self, index_name: str) -> str:
        indexes = self.check_indexes()
        if index_name in indexes:
            self.__query('drop_index', index_name)
            return f'{index_name} dropped.'
        raise MongoIndexError(f'invalid index name - {index_name}')

    def count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        if getattr(self._mongo_model._collection, 'count_documents'):
            return self.__query(
                'count_documents',
                logical_query or query,
                session=session,
                logical=bool(logical_query),
            )
        return self.__query(
            'count',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )

    def find_one(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Any:
        sort, sort_fields = sort_validation(sort, sort_fields)
        data = self.__query(
            'find_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
            sort=[(field, sort or 1) for field in sort_fields] if sort_fields else None,
        )
        if data:
            obj = self._mongo_model.parse_obj(data)
            return obj
        return None

    def _find(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Generator:
        data = self.__query(
            'find', logical_query or query, session=session, logical=bool(logical_query)
        )
        if skip_rows is not None:
            data = data.skip(skip_rows)
        if limit_rows:
            data = data.limit(limit_rows)
        sort, sort_fields = sort_validation(sort, sort_fields)
        return (
            data.sort([(field, sort or 1) for field in sort_fields])
            if sort_fields
            else data
        )

    def find(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> QuerySet:
        data = self._find(
            logical_query, skip_rows, limit_rows, session, sort_fields, sort, **query
        )
        return QuerySet(self._mongo_model, data)

    def find_with_count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> tuple:
        count = self.count(session=session, logical_query=logical_query, **query,)
        results = self.find(
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            session=session,
            logical_query=logical_query,
            sort_fields=sort_fields,
            sort=sort,
            **query,
        )
        return count, results

    def insert_one(self, session: Optional[ClientSession] = None, **query) -> ObjectId:
        obj = self._mongo_model.parse_obj(query)
        data = self.__query('insert_one', obj.query_data, session=session)
        return data.inserted_id

    def insert_many(self, data: List, session: Optional[ClientSession] = None) -> int:
        parse_obj = self._mongo_model.parse_obj
        query = [
            parse_obj(obj).query_data if isinstance(obj, dict) else obj.query_data
            for obj in data
        ]
        r = self.__query('insert_many', query, session=session)
        return len(r.inserted_ids)

    def delete_one(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:

        r = self.__query(
            'delete_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    def delete_many(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        *args,
        **query,
    ) -> int:

        r = self.__query(
            'delete_many',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    def _ensure_update_data(self, **fields) -> tuple:
        if not any("__set" in f for f in fields):
            raise ValueError("not fields for updating!")
        queries = {}
        set_values = {}
        for name, value in fields.items():
            if name.endswith('__set'):
                name = name.replace('__set', '')
                data = self._mongo_model._validate_query_data({name: value})
                set_values.update(data)
            else:
                queries.update({name: value})
        return queries, set_values

    def replace_one(
        self,
        replacement: Dict,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **filter_query,
    ) -> Any:
        if not filter_query:
            raise AttributeError('not filter parameters')
        if not replacement:
            raise AttributeError('not replacement parameters')
        return self.__query(
            'replace_one',
            self._mongo_model._validate_query_data(filter_query),
            replacement=self._mongo_model._validate_query_data(replacement),
            upsert=upsert,
            session=session,
        )

    def get(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Any:
        obj = self.find_one(
            logical_query=logical_query,
            session=session,
            sort_fields=sort_fields,
            sort=sort,
            **query,
        )
        if not obj:
            raise DoesNotExist(self._mongo_model.__name__)  # type: ignore
        return obj

    def __validate_raw_query(
        self, method_name: str, raw_query: Union[Dict, List[Dict], Tuple[Dict]]
    ) -> tuple:
        if (
            'insert' in method_name
            or 'replace' in method_name
            or 'update' in method_name
        ):
            if isinstance(raw_query, list):
                raw_query = list(map(self._mongo_model._validate_query_data, raw_query))
            elif isinstance(raw_query, dict):
                raw_query = self._mongo_model._validate_query_data(raw_query)
            else:
                params = [
                    query[key] if '$' in key else query
                    for query in raw_query
                    for key in query.keys()
                ]
                map(self._mongo_model._validate_query_data, params)
        parsed_query = raw_query if isinstance(raw_query, tuple) else (raw_query,)
        return parsed_query

    def get_or_create(self, **query) -> Tuple:
        defaults = query.pop('defaults', {})
        obj = self.find_one(**query)
        if obj:
            created = False
        else:
            created = True
            inserted_id = self.insert_one(**{**query, **defaults})
            obj = self.find_one(_id=inserted_id)
        return obj, created

    def update_or_create(self, **query) -> Tuple:
        defaults = query.pop('defaults', {})
        obj = self.find_one(**query)
        if obj:
            created = False
            for field, value in defaults.items():
                setattr(obj, field, value)
            obj.save()
        else:
            created = True
            inserted_id = self.insert_one(**{**query, **defaults})
            obj = self.find_one(_id=inserted_id)
        return obj, created

    def raw_query(
        self,
        method_name: str,
        raw_query: Union[Dict, List[Dict], Tuple[Dict]],
        session: Optional[ClientSession] = None,
    ) -> Any:
        parsed_query = self.__validate_raw_query(method_name, raw_query)
        query = getattr(self._mongo_model._collection, method_name)
        return query(*parsed_query, session=session)

    def _update(
        self,
        method: str,
        query: Dict,
        upsert: bool = True,
        session: Optional[ClientSession] = None,
    ) -> int:
        query, set_values = self._ensure_update_data(**query)
        r = self.__query(
            method, query, {'$set': set_values}, upsert=upsert, session=session
        )
        return r.modified_count

    def update_one(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        return self._update('update_one', query, upsert=upsert, session=session)

    def update_many(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        return self._update('update_many', query, upsert=upsert, session=session)

    def distinct(
        self, field: str, session: Optional[ClientSession] = None, **query
    ) -> Union[list, dict]:
        query = self._mongo_model._validate_query_data(query)
        method = getattr(self._mongo_model._collection, 'distinct')
        return method(key=field, filter=query, session=session)

    def raw_aggregate(self, data: Any, session: Optional[ClientSession] = None) -> list:
        return list(self.__query("aggregate", data, session=session))

    def _aggregate(self, *args, **query) -> dict:
        session = query.pop('session', None)
        aggregation = query.pop('aggregation', None)
        group_by = query.pop('group_by', None)
        if not aggregation and not group_by:
            raise ValueError('miss aggregation or group_by')
        if isinstance(aggregation, Iterable):
            aggregate_query = {}
            for agg in aggregation:
                aggregate_query.update(agg._aggregate_query(self._mongo_model))
        elif aggregation is not None:
            aggregate_query = aggregation._aggregate_query(self._mongo_model)
        else:
            aggregate_query = {}
        if group_by:
            group_by = group_by_aggregate_generation(group_by)
            aggregate_query.pop('_id', None)
            group_params = {"$group": {"_id": group_by, **aggregate_query}}
        else:
            group_params = {
                "$group": {"_id": None, **aggregate_query}
                if '_id' not in aggregate_query
                else aggregate_query
            }
        data = [
            {
                "$match": self._mongo_model._validate_query_data(query)
                if not args
                else self._mongo_model._check_query_args(*args)
            },
            group_params,
        ]
        result = list(self.__query("aggregate", data, session=session))
        if not result:
            return {}
        result_data = {}
        for r in result:
            name = generate_name_field(r.pop('_id'))
            result_data.update({name: r} if name else r)
        return result_data

    def simple_aggregate(self, *args, **kwargs):
        return self._aggregate(*args, **kwargs)

    def aggregate_sum(self, agg_field: str, **query) -> dict:
        return self._aggregate(aggregation=Sum(agg_field), **query).get(
            f'{agg_field}__sum', 0
        )

    def aggregate_max(self, agg_field: str, **query) -> dict:
        return self._aggregate(aggregation=Max(agg_field), **query).get(
            f'{agg_field}__max', 0
        )

    def aggregate_min(self, agg_field: str, **query) -> dict:
        return self._aggregate(aggregation=Min(agg_field), **query).get(
            f'{agg_field}__min', 0
        )

    def aggregate_avg(self, agg_field: str, **query) -> dict:
        return self._aggregate(aggregation=Avg(agg_field), **query).get(
            f'{agg_field}__avg', 0
        )

    def _bulk_operation(
        self,
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
                self.__query('bulk_write', data, session=session)
            return None
        data = bulk_query_generator(
            models,
            updated_fields=updated_fields,
            query_fields=query_fields,
            upsert=upsert,
        )
        self.__query('bulk_write', data, session=session)

    def bulk_update(
        self,
        models: List,
        updated_fields: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not updated_fields:
            raise ValidationError('updated_fields cannot be empty')
        return self._bulk_operation(
            models,
            updated_fields=updated_fields,
            batch_size=batch_size,
            session=session,
        )

    def bulk_create(
        self,
        models: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> int:
        if batch_size is None or batch_size <= 0:
            batch_size = 30000
        result = 0
        for data in chunk_by_length(models, batch_size):
            result += self.insert_many(data, session=session)
        return result

    def bulk_update_or_create(
        self,
        models: List,
        query_fields: List,
        batch_size: Optional[int] = 10000,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not query_fields:
            raise ValidationError('query_fields cannot be empty')
        return self._bulk_operation(
            models,
            query_fields=query_fields,
            batch_size=batch_size,
            upsert=True,
            session=session,
        )

    def _find_with_replacement_or_with_update(
        self,
        operation: str,
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        filter_, set_values = self._ensure_update_data(**query)
        return_document = ReturnDocument.AFTER
        replacement = query.pop('replacement', None)

        projection = {f: True for f in projection_fields} if projection_fields else None
        extra_params = {
            'return_document': return_document,
            'projection': projection,
            'upsert': upsert,
            'session': session,
        }
        if sort_fields:
            extra_params['sort'] = [(field, sort or 1) for field in sort_fields]

        if replacement:
            extra_params['replacement'] = replacement

        data = self.__query(operation, filter_, {'$set': set_values}, **extra_params)
        if projection:
            return {
                field: value for field, value in data.items() if field in projection
            }
        return self._mongo_model.parse_obj(data)

    def find_one_and_update(
        self,
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ):

        return self._find_with_replacement_or_with_update(
            'find_one_and_update',
            projection_fields=projection_fields,
            sort_fields=[(field, sort or 1) for field in sort_fields]
            if sort_fields
            else None,
            sort=sort,
            upsert=upsert,
            session=session,
            **query,
        )

    def find_and_replace(
        self,
        replacement: Union[dict, Any],
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        if not isinstance(replacement, dict):
            replacement = replacement.query_data
        return self._find_with_replacement_or_with_update(
            'find_and_replace',
            projection_fields=projection_fields,
            sort_fields=[(field, sort) for field in sort_fields]
            if sort_fields
            else None,
            sort=sort,
            upsert=upsert,
            session=session,
            replacement=replacement,
            **query,
        )

    def drop_collection(self, force: bool = False) -> str:
        drop_message = f'{self._mongo_model.__name__.lower()} - dropped!'  # type: ignore
        if force:
            self.__query('drop', query_params={})
            return drop_message
        value = input(
            f'Are u sure for drop this collection - {self._mongo_model.__name__.lower()} (y, n)'  # type: ignore
        )
        if value.lower() == 'y':
            self.__query('drop', query_params={})
            return drop_message
        return 'nope'


class QueryBuilder(BaseQueryBuilder):
    pass


class AsyncQueryBuilder(BaseQueryBuilder):
    def __getattribute__(self, name: str) -> Any:
        declared_methods = (
            'find_one',
            '_find',
            'delete_one',
            'delete_many',
            'insert_one',
            'insert_many',
            'update_one',
            'update_many',
            'count',
            'distinct',
            'raw_aggregate',
            'raw_query',
            'replace_one',
            '__query',
            '_find_with_replacement_or_with_update',
            '_aggregate',
            '_bulk_operation',
        )
        if name in declared_methods:
            return sync_to_async(super().__getattribute__(name))
        return super().__getattribute__(name)

    async def find(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> QuerySet:
        data = await self._find(
            logical_query, skip_rows, limit_rows, session, sort_fields, sort, **query
        )
        return QuerySet(self._mongo_model, data)

    async def get_or_create(self, **query) -> Tuple:
        defaults = query.pop('defaults', {})
        obj = await self.find_one(**query)
        if obj:
            created = False
        else:
            created = True
            inserted_id = await self.insert_one(**{**query, **defaults})
            obj = await self.find_one(_id=inserted_id)
        return obj, created

    async def simple_aggregate(self, *args, **kwargs):
        return await self._aggregate(*args, **kwargs)

    async def aggregate_sum(self, agg_field: str, **query) -> dict:
        result = await self._aggregate(aggregation=Sum(agg_field), **query)
        return result.get(f'{agg_field}__sum', 0)

    async def aggregate_max(self, agg_field: str, **query) -> dict:
        result = await self._aggregate(aggregation=Max(agg_field), **query)
        return result.get(f'{agg_field}__max', 0)

    async def aggregate_min(self, agg_field: str, **query) -> dict:
        result = await self._aggregate(aggregation=Min(agg_field), **query)
        return result.get(f'{agg_field}__min', 0)

    async def aggregate_avg(self, agg_field: str, **query) -> dict:
        result = self._aggregate(aggregation=Avg(agg_field), **query)
        return result.get(f'{agg_field}__avg', 0)

    async def bulk_create(
        self,
        models: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> int:
        if batch_size is None or batch_size <= 0:
            batch_size = 30000
        result = 0
        for data in chunk_by_length(models, batch_size):
            result += await self.insert_many(data, session=session)
        return result

    async def bulk_update(
        self,
        models: List,
        updated_fields: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not updated_fields:
            raise ValidationError('updated_fields cannot be empty')
        return await self._bulk_operation(
            models,
            updated_fields=updated_fields,
            batch_size=batch_size,
            session=session,
        )

    async def bulk_update_or_create(
        self,
        models: List,
        query_fields: List,
        batch_size: Optional[int] = 10000,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not query_fields:
            raise ValidationError('query_fields cannot be empty')
        return await self._bulk_operation(
            models,
            query_fields=query_fields,
            batch_size=batch_size,
            upsert=True,
            session=session,
        )

    async def get(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Any:
        obj = await self.find_one(
            logical_query=logical_query,
            session=session,
            sort_fields=sort_fields,
            sort=sort,
            **query,
        )
        if not obj:
            raise DoesNotExist(self._mongo_model.__name__)  # type: ignore
        return obj

    async def find_and_replace(
        self,
        replacement: Union[dict, Any],
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        if not isinstance(replacement, dict):
            replacement = replacement.query_data
        return await self._find_with_replacement_or_with_update(
            'find_and_replace',
            projection_fields=projection_fields,
            sort_fields=[(field, sort) for field in sort_fields]
            if sort_fields
            else None,
            sort=sort,
            upsert=upsert,
            session=session,
            replacement=replacement,
            **query,
        )

    async def update_or_create(self, **query) -> Tuple:
        defaults = query.pop('defaults', {})
        obj = await self.find_one(**query)
        if obj:
            created = False
            for field, value in defaults.items():
                setattr(obj, field, value)
            obj.save()
        else:
            created = True
            inserted_id = await self.insert_one(**{**query, **defaults})
            obj = await self.find_one(_id=inserted_id)
        return obj, created

    async def find_with_count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> tuple:
        count = await self.count(session=session, logical_query=logical_query, **query,)
        results = await self.find(
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            session=session,
            logical_query=logical_query,
            sort_fields=sort_fields,
            sort=sort,
            **query,
        )
        return count, results
