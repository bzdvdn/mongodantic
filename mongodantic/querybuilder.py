from typing import Union, List, Dict, Optional, Any, Tuple
from collections.abc import Iterable
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
    generate_name_field,
)
from .queryset import QuerySet
from .logical import LogicalCombination, Query
from .aggregation import Lookup, LookupCombination


class QueryBuilder(object):
    def __init__(self):
        self._mongo_model = None

    def add_model(self, mongo_model):
        if not self._mongo_model:
            self._mongo_model = mongo_model

    def __query(
        self,
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
            query_params = self._mongo_model._check_query_args(query_params)
        elif isinstance(query_params, dict):
            query_params = self._mongo_model._validate_query_data(query_params)

        method = getattr(self._mongo_model.collection, method_name)
        query = [query_params]
        if session:
            kwargs['session'] = session
        if set_values:
            query = [query_params, set_values]
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
            self._mongo_model._reconnect()
            if counter >= 5:
                raise MongoConnectionError(str(description))
            counter += 1
            return self.__query(
                method_name=method_name,
                query_params=inner_query_params,
                set_values=set_values,
                session=session,
                counter=counter,
                logical=logical,
                **kwargs,
            )

    def check_indexes(self) -> List:
        index_list = list(self.__query('list_indexes', {}))
        return_list = []
        for index in index_list:
            d = dict(index)
            _dict = {'name': d['name'], 'key': dict(d['key'])}
            return_list.append(_dict)
        return return_list

    def add_index(
        self,
        index_name: str,
        index_type: int,
        background: bool = True,
        unique: bool = False,
        sparse: bool = False,
    ) -> str:
        indexes = [index['name'] for index in self.check_indexes()]
        if f'{index_name}_{index_type}' in indexes:
            raise MongoIndexError(f'{index_name} - already exists.')
        try:
            self.__query(
                'create_index',
                [(index_name, index_type)],
                background=background,
                unique=unique,
                sparse=sparse,
            )
            return f'index with name - {index_name}_{index_type} created.'
        except Exception as e:
            raise MongoIndexError(f'detail: {str(e)}')

    def add_compound_index(
        self,
        background: bool = True,
        unique: bool = False,
        sparse: bool = False,
        **indexes,
    ) -> str:
        if not indexes:
            raise ValueError('miss indexes')
        index_name = f'_'.join(f'{name}_{type_}' for name, type_ in indexes.items())
        db_indexes = [index['name'] for index in self.check_indexes()]
        if index_name in db_indexes:
            raise MongoIndexError(f'{index_name} - already exists.')
        try:
            self.__query(
                'create_index',
                [
                    (index_name, index_type)
                    for index_name, index_type in indexes.items()
                ],
                background=background,
                unique=unique,
                sparse=sparse,
            )
            return f'index with name - {index_name} created.'
        except Exception as e:
            raise MongoIndexError(f'detail: {str(e)}')

    def drop_index(self, index_name: str, index_type: int) -> str:
        indexes = self.check_indexes()
        drop = False
        for index in indexes:
            if f'{index_name}_{index_type}' in index['name']:
                drop = True
                self.__query('drop_index', index['name'])
        if drop:
            return f'{index_name}_{index_type} dropped.'
        raise MongoIndexError(
            f'invalid index name - {index_name} and index type - {index_type}'
        )

    def count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        if getattr(self._mongo_model.collection, 'count_documents'):
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
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        **query,
    ) -> Any:

        data = self.__query(
            'find_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
            sort=[(field, sort) for field in sort_fields] if sort_fields else None,
        )
        if data:
            obj = self._mongo_model.parse_obj(data)
            return obj
        return None

    def find(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        **query,
    ) -> QuerySet:
        data = self.__query(
            'find', logical_query or query, session=session, logical=bool(logical_query)
        )
        if skip_rows is not None:
            data = data.skip(skip_rows)
        if limit_rows:
            data = data.limit(limit_rows)
        if sort not in (1, -1):
            raise ValueError(f'invalid sort value must be 1 or -1 not {sort}')
        print('==== sort fields - ', sort_fields)

        return QuerySet(
            self._mongo_model,
            data.sort([(field, sort) for field in sort_fields])
            if sort_fields
            else data,
        )

    def find_with_count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        **query,
    ) -> tuple:
        count = self.count(session=session, logical_query=logical_query, **query,)
        results = self.find(
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            session=session,
            logical_query=logical_query,
            sort_fields=[(field, sort) for field in sort_fields]
            if sort_fields
            else None,
            sort=sort,
            **query,
        )
        return count, results

    def insert_one(self, session: Optional[ClientSession] = None, **query) -> ObjectId:
        obj = self._mongo_model.parse_obj(query)
        data = self.__query('insert_one', obj.data, session=session)
        return data.inserted_id

    def insert_many(self, data: List, session: Optional[ClientSession] = None) -> int:
        parse_obj = self._mongo_model.parse_obj
        query = [
            parse_obj(obj).data if isinstance(obj, ModelMetaclass) else obj.data
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

    def raw_query(
        self,
        method_name: str,
        raw_query: Union[Dict, List[Dict], Tuple[Dict]],
        session: Optional[ClientSession] = None,
    ) -> Any:
        parsed_query = self.__validate_raw_query(method_name, raw_query)
        query = getattr(self._mongo_model.collection, method_name)
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

    def aggregate(self, *args, **query) -> dict:
        session = query.pop('session', None)
        aggregation = query.pop('aggregation', None)
        group_by = query.pop('group_by', None)
        if not aggregation:
            raise ValueError('miss aggregation')
        if isinstance(aggregation, Iterable):
            aggregate_query = {}
            for agg in aggregation:
                aggregate_query.update(agg._aggregate_query(self._mongo_model))
        else:
            aggregate_query = aggregation._aggregate_query(self._mongo_model)
        if group_by:
            group_by = (
                {g: f'${g}' for g in group_by}
                if isinstance(group_by, (list, tuple))
                else f'${group_by}'
            )
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

    def aggregate_lookup(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        lookup: Union[Lookup, LookupCombination, None] = None,
        project: Optional[dict] = None,
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Union[QuerySet, list]:
        if not lookup:
            raise ValueError('invalid lookup param')
        query_params = [
            {
                '$match': self._mongo_model._check_query_args(logical_query)
                if logical_query
                else self._mongo_model._validate_query_data(query)
            }
        ]
        accepted_lookup, reference_models = lookup.accept(self._mongo_model, project)
        query_params.extend(accepted_lookup)
        if sort_fields:
            query_params.append({'$sort': {sf: sort for sf in sort_fields}})
        if limit_rows:
            query_params.append({'$limit': limit_rows})
        data = self.__query(
            "aggregate", query_params, session=session, logical=bool(logical_query)
        )
        if skip_rows:
            data = data.skip(skip_rows)
        if project:
            return list(data)
        return QuerySet(self._mongo_model, data, reference_models)

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
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
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
            extra_params['sort'] = [(field, sort) for field in sort_fields]

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
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ):

        return self._find_with_replacement_or_with_update(
            'find_one_and_update',
            projection_fields=projection_fields,
            sort_fields=[(field, sort) for field in sort_fields]
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
        sort_fields: Optional[Union[tuple, list]] = None,
        sort: int = 1,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Any:
        if not isinstance(replacement, dict):
            replacement = replacement.data
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
        drop_message = f'{self._mongo_model.__name__.lower()} - dropped!'
        if force:
            self.__query('drop', query_params={})
            return drop_message
        value = input(
            f'Are u sure for drop this collection - {self._mongo_model.__name__.lower()} (y, n)'
        )
        if value.lower() == 'y':
            self.__query('drop', query_params={})
            return drop_message
        return 'nope'
