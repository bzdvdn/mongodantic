from typing import (
    Union,
    List,
    Dict,
    Optional,
    Any,
    Tuple,
    TYPE_CHECKING,
    Generator,
    no_type_check,
)
from collections.abc import Iterable
from pymongo import ReturnDocument
from pymongo import IndexModel
from pymongo.client_session import ClientSession
from bson import ObjectId

from .exceptions import (
    MongoValidationError,
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

__all__ = ('QueryBuilder', 'AsyncQueryBuilder')


class QueryBuilder(object):
    def __init__(self, mongo_model: 'MongoModel'):
        self._mongo_model: 'MongoModel' = mongo_model

    @handle_and_convert_connection_errors
    def __query(
        self,
        method_name: str,
        query_params: Union[List, Dict, str, Query, LogicalCombination, None],
        set_values: Optional[Dict] = None,
        session: Optional[ClientSession] = None,
        logical: bool = False,
        **kwargs,
    ) -> Any:
        """main query function

        Args:
            method_name (str): query method like find, find_one and other
            query_params (Union[List, Dict, str, Query, LogicalCombination, None]): query params: dict or Query or LogicalCombination
            set_values (Optional[Dict], optional): for updated method. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
            logical (bool, optional): if logical. Defaults to False.

        Returns:
            Any: query result
        """
        method = getattr(self._mongo_model._collection, method_name)

        # Special cases that don't need query validation
        if method_name in ('list_indexes', 'drop'):
            if session:
                kwargs['session'] = session
            return method(**kwargs)

        if logical:
            query_params = self._mongo_model._check_query_args(query_params)
        elif isinstance(query_params, dict):
            query_params = self._mongo_model._validate_query_data(query_params)

        query: tuple = (query_params,)
        if session:
            kwargs['session'] = session
        if set_values:
            query = (query_params, set_values)
        if kwargs:
            return method(*query, **kwargs)
        return method(*query)

    def check_indexes(self) -> None:
        """Check indexes for model"""
        try:
            self._mongo_model.collection.list_indexes()
        except Exception:
            pass

    def create_indexes(
        self,
        indexes: List[IndexModel],
        session: Optional[ClientSession] = None,
    ) -> List[str]:
        return self.__query('create_indexes', indexes, session=session)

    def drop_index(self, index_name: str) -> str:
        """Drop an index by name

        Args:
            index_name (str): Name of the index to drop

        Returns:
            str: Success message

        Raises:
            MongoIndexError: If index name is invalid
        """
        try:
            self.__query('drop_index', index_name)
            return f'{index_name} dropped.'
        except Exception as e:
            raise MongoIndexError(
                f'invalid index name - {index_name}: {str(e)}')

    def count(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """count query

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query | LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: count of documents
        """
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

    def count_documents(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """count query

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query | LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: count of documents
        """
        return self.count(logical_query, session, **query)

    def find_one(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Optional['MongoModel']:
        """find one document

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query | LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): iterable from sort fielda. Defaults to None.
            sort (Optional[int], optional): sort value -1 or 1. Defaults to None.

        Returns:
            Optional[MongoModel]: MongoModel instance or None
        """
        sort, sort_fields = sort_validation(sort, sort_fields)
        data = self.__query(
            'find_one',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
            sort=[(field, sort or 1)
                  for field in sort_fields] if sort_fields else None,
        )
        if data:
            obj = self._mongo_model.model_validate(data)
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
        """find method

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query|LogicalCombunation. Defaults to None.
            skip_rows (Optional[int], optional): skip rows for pagination. Defaults to None.
            limit_rows (Optional[int], optional): limit rows. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): iterable from sort fielda. Defaults to None.
            sort (Optional[int], optional): sort value -1 or 1. Defaults to None.

        Returns:
            QuerySet: Mongodantic QuerySet
        """
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
        """find and count

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query|LogicalCombination or None. Defaults to None.
            skip_rows (Optional[int], optional): for pagination. Defaults to None.
            limit_rows (Optional[int], optional): for pagination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): field for sort. Defaults to None.
            sort (Optional[int], optional): sort value. Defaults to None.

        Returns:
            tuple: count of query data, QuerySet
        """
        count = self.count(
            session=session,
            logical_query=logical_query,
            **query,
        )
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

    def insert_one(self, session: Optional[ClientSession] = None, **obj) -> ObjectId:
        obj = self._mongo_model.model_validate(obj)
        data = self.__query('insert_one', obj.query_data, session=session)
        return data.inserted_id

    def insert_many(
        self,
        data: List[Union[Dict, 'MongoModel']],
        session: Optional[ClientSession] = None,
        **kwargs,
    ) -> int:
        objects = []
        for obj in data:
            if isinstance(obj, dict):
                obj = self._mongo_model.model_validate(obj)
            objects.append(obj.query_data)
        data = self.__query('insert_many', objects, session=session)
        return len(data.inserted_ids)

    def delete_one(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """delete one document

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query|LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: deleted documents count
        """
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
        **query,
    ) -> int:
        """delete many document

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query|LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: deleted documents count
        """
        r = self.__query(
            'delete_many',
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    def _prepare_update_data(self, **fields) -> tuple:
        """prepare and validate query data for update queries"""

        if not any("__set" in f for f in fields):
            raise MongoValidationError("not fields for updating!")
        query_params = {}
        set_values = {}
        for name, value in fields.items():
            if name.endswith('__set'):
                name = name.replace('__set', '')
                data = self._mongo_model._validate_query_data({name: value})
                set_values.update(data)
            else:
                query_params.update({name: value})
        return query_params, set_values

    def replace_one(
        self,
        replacement: Dict,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **filter_query,
    ) -> Any:
        """replace one

        Args:
            replacement (Dict): replacement object
            upsert (bool, optional): pymongo upsert. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Raises:
            MongoValidationError: if not filter query
            MongoValidationError: if not replacement obj

        Returns:
            Any: pymongo replace_one query result
        """
        if not filter_query:
            raise MongoValidationError('not filter parameters')
        if not replacement:
            raise MongoValidationError('not replacement parameters')
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
        """method like django orm get"""

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
            try:
                if isinstance(raw_query, list):
                    raw_query = list(
                        map(self._mongo_model._validate_query_data, raw_query))
                elif isinstance(raw_query, dict):
                    raw_query = self._mongo_model._validate_query_data(
                        raw_query)
                else:
                    params = [
                        query[key] if '$' in key else query
                        for query in raw_query
                        for key in query.keys()
                    ]
                    map(self._mongo_model._validate_query_data, params)
            except Exception as e:
                # Convert any validation error to MongoValidationError
                if isinstance(e, MongoValidationError):
                    raise
                raise MongoValidationError(str(e))
        parsed_query = raw_query if isinstance(
            raw_query, tuple) else (raw_query,)
        return parsed_query

    def get_or_create(self, **query) -> Tuple:
        """like django orm get_or_create

        Returns:
            Tuple: MongoModel instance, True/False
        """
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
        """like django orm update_or_create

        Returns:
            Tuple: MongoModel instance, True/False
        """
        defaults = query.pop('defaults', {})
        obj = self.find_one(**query)
        if obj is not None:
            created = False
            for field, value in defaults.items():
                setattr(obj, field, value)
        else:
            created = True
            obj = self._mongo_model(**{**query, **defaults})  # type: ignore
        obj.save()
        return obj, created

    def raw_query(
        self,
        method_name: str,
        raw_query: Union[Dict, List[Dict], Tuple[Dict]],
        session: Optional[ClientSession] = None,
    ) -> Any:
        """pymongo raw query

        Args:
            method_name (str): pymongo method, like insert_one
            raw_query (Union[Dict, List[Dict], Tuple[Dict]]): query data
            session (Optional[ClientSession], optional): pymongo Session. Defaults to None.

        Raises:
            MongoValidationError: raise if invalid data

        Returns:
            Any: pymongo query result
        """
        try:
            # Validate the data before executing the query
            if 'insert' in method_name:
                if isinstance(raw_query, dict):
                    self._mongo_model.model_validate(raw_query)
                elif isinstance(raw_query, list):
                    for item in raw_query:
                        self._mongo_model.model_validate(item)

            parsed_query = self.__validate_raw_query(method_name, raw_query)
            query = getattr(self._mongo_model._collection, method_name)
            return query(*parsed_query, session=session)
        except AttributeError:
            raise MongoValidationError('invalid method name')
        except Exception as e:
            if isinstance(e, MongoValidationError):
                raise
            raise MongoValidationError(str(e))

    def _update(
        self,
        method: str,
        query: Dict,
        upsert: bool = True,
        session: Optional[ClientSession] = None,
    ) -> int:
        """innert method for update

        Args:
            method (str): one of update_many or update_one
            query (Dict): update query
            upsert (bool, optional): upsert option. Defaults to True.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: updated documents count
        """
        query, set_values = self._prepare_update_data(**query)
        r = self.__query(
            method, query, {'$set': set_values}, upsert=upsert, session=session
        )
        return r.modified_count

    def update_one(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        """update one document

        Args:
            upsert (bool, optional): pymongo upsert. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: updated documents count
        """
        return self._update('update_one', query, upsert=upsert, session=session)

    def update_many(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        """update many document

        Args:
            upsert (bool, optional): pymongo upsert. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: updated documents count
        """
        return self._update('update_many', query, upsert=upsert, session=session)

    def distinct(
        self, field: str, session: Optional[ClientSession] = None, **query
    ) -> list:
        """wrapper for pymongo distinct

        Args:
            field (str): distinct field
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            list: list of distinct values
        """
        query = self._mongo_model._validate_query_data(query)
        method = getattr(self._mongo_model._collection, 'distinct')
        return method(key=field, filter=query, session=session)

    def raw_aggregate(
        self, data: List[Dict[Any, Any]], session: Optional[ClientSession] = None
    ) -> list:
        """raw aggregation query

        Args:
            data (List[Dict[Any, Any]]): aggregation query
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            list: aggregation result
        """
        return list(self.__query("aggregate", data, session=session))

    def _aggregate(self, *args, **query) -> dict:
        """main aggregate method

        Raises:
            MongoValidationError: miss aggregation or group_by

        Returns:
            dict: aggregation result
        """
        session = query.pop('session', None)
        aggregation = query.pop('aggregation', None)
        group_by = query.pop('group_by', None)
        if not aggregation and not group_by:
            raise MongoValidationError('miss aggregation or group_by')
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

    def simple_aggregate(self, *args, **kwargs) -> dict:
        return self._aggregate(*args, **kwargs)

    def aggregate_sum(self, agg_field: str, **query) -> int:
        return self._aggregate(aggregation=Sum(agg_field), **query).get(
            f'{agg_field}__sum', 0
        )

    def aggregate_max(self, agg_field: str, **query) -> int:
        return self._aggregate(aggregation=Max(agg_field), **query).get(
            f'{agg_field}__max', 0
        )

    def aggregate_min(self, agg_field: str, **query) -> int:
        return self._aggregate(aggregation=Min(agg_field), **query).get(
            f'{agg_field}__min', 0
        )

    def aggregate_avg(self, agg_field: str, **query) -> int:
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
        """base bulk operation method

        Args:
            models (List): MongoModels objects
            updated_fields (Optional[List], optional): list of updated fields. Defaults to None.
            query_fields (Optional[List], optional): list of query fields. Defaults to None.
            batch_size (Optional[int], optional): query batch. Defaults to 10000.
            upsert (bool, optional): for upsert pymongo queries. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
        """
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
        """bulk update method

        Args:
            models (List): MongoModel objects
            updated_fields (List): list of updated fields, like ['name', 'last_name']
            batch_size (Optional[int], optional): query batch. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Raises:
            MongoValidationError: if invalid param
        """
        if not updated_fields:
            raise MongoValidationError('updated_fields cannot be empty')
        self._bulk_operation(
            models,
            updated_fields=updated_fields,
            batch_size=batch_size
            if batch_size is not None and batch_size > 0
            else 10000,
            session=session,
        )

    def bulk_create(
        self,
        models: List,
        batch_size: Optional[int] = 30000,
        session: Optional[ClientSession] = None,
        _ordered: bool = True,
        _bypass_document_validation: bool = False,
    ) -> int:
        """bulk create method

        Args:
            models (List): MongoModels obejcts
            batch_size (Optional[int], optional): query batch. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            int: count of objects created
        """
        if batch_size is None or batch_size <= 0:
            batch_size = 30000
        result = 0
        for data in chunk_by_length(models, batch_size):
            result += self.insert_many(
                data,
                session=session,
                _ordered=_ordered,
                _bypass_document_validation=_bypass_document_validation,
            )
        return result

    def bulk_update_or_create(
        self,
        models: List,
        query_fields: List,
        batch_size: Optional[int] = 10000,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Method for update/create rows

        Args:
            models (List): List of MongoModels objects
            query_fields (List): list of query fields like ['name'], perfect if this fields in indexes
            batch_size (Optional[int], optional): query obejcts batch. Defaults to 10000.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Raises:
            MongoValidationError: if invalid models

        """
        if not query_fields:
            raise MongoValidationError('query_fields cannot be empty')
        self._bulk_operation(
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
    ) -> Union[Dict, 'MongoModel']:
        """base method for find_with_<operation>

        Args:
            operation (str): operation name
            projection_fields (Optional[list], optional): prejection. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): sort fields. Defaults to None.
            sort (Optional[int], optional): -1 or 1. Defaults to None.
            upsert (bool, optional): True/False. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            Union[Dict, 'MongoModel']: MongoModel or Dict
        """
        filter_, set_values = self._prepare_update_data(**query)
        return_document = ReturnDocument.AFTER
        replacement = query.pop('replacement', None)

        projection = {
            f: True for f in projection_fields} if projection_fields else None
        extra_params = {
            'return_document': return_document,
            'projection': projection,
            'upsert': upsert,
            'session': session,
        }
        if sort_fields:
            extra_params['sort'] = [(field, sort or 1)
                                    for field in sort_fields]

        if replacement:
            extra_params['replacement'] = replacement

        data = self.__query(operation, filter_, {
                            '$set': set_values}, **extra_params)
        if projection:
            return {
                field: value for field, value in data.items() if field in projection.keys()
            }
        return self._mongo_model.model_validate(data)

    def find_one_and_update(
        self,
        session: Optional[ClientSession] = None,
        projection_fields: Optional[List] = None,
        **query,
    ) -> Optional[Union['MongoModel', Dict]]:
        filter_, set_values = self._prepare_update_data(**query)
        if projection_fields:
            projection = {field: 1 for field in projection_fields}
            data = self.__query(
                'find_one_and_update',
                filter_,
                {'$set': set_values},
                session=session,
                projection=projection,
                return_document=ReturnDocument.AFTER,
            )
            return data
        data = self.__query(
            'find_one_and_update',
            filter_,
            {'$set': set_values},
            session=session,
            return_document=ReturnDocument.AFTER,
        )
        if data:
            return self._mongo_model.model_validate(data)
        return None

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
        """find one and replace

        Args:
            operation (str): operation name
            projection_fields (Optional[list], optional): prejection. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): sort fields. Defaults to None.
            sort (Optional[int], optional): -1 or 1. Defaults to None.
            upsert (bool, optional): True/False. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            Union[Dict, 'MongoModel']: MongoModel or Dict
        """
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

    def drop_collection(self, force: bool = False, session: Optional[ClientSession] = None) -> str:
        """drop collection

        Args:
            force (bool, optional): if u wanna force drop. Defaults to False.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.

        Returns:
            str: result message
        """
        drop_message = f'{self._mongo_model.__name__.lower()} - dropped!'  # type: ignore
        if force:
            self.__query('drop', query_params=None, session=session)
            return drop_message
        value = input(
            # type: ignore
            f'Are u sure for drop this collection - {self._mongo_model.__name__.lower()} (y, n)'
        )
        if value.lower() == 'y':
            self.__query('drop', query_params=None, session=session)
            return drop_message
        return 'nope'


class AsyncQueryBuilder(QueryBuilder):
    @sync_to_async
    def __query(self, *args, **kwargs):
        return QueryBuilder._QueryBuilder__query(self, *args, **kwargs)

    @sync_to_async
    def insert_one(self, *args, **kwargs):
        return super().insert_one(*args, **kwargs)

    @sync_to_async
    def insert_many(self, *args, **kwargs):
        return super().insert_many(*args, **kwargs)

    @sync_to_async
    def delete_one(self, *args, **kwargs):
        return super().delete_one(*args, **kwargs)

    @sync_to_async
    def delete_many(self, *args, **kwargs):
        return super().delete_many(*args, **kwargs)

    @sync_to_async
    def update_one(self, *args, **kwargs):
        return super().update_one(*args, **kwargs)

    @sync_to_async
    def update_many(self, *args, **kwargs):
        return super().update_many(*args, **kwargs)

    @sync_to_async
    def distinct(self, *args, **kwargs):
        return super().distinct(*args, **kwargs)

    @sync_to_async
    def raw_aggregate(self, *args, **kwargs):
        return super().raw_aggregate(*args, **kwargs)

    @sync_to_async
    def raw_query(self, *args, **kwargs):
        return super().raw_query(*args, **kwargs)

    @sync_to_async
    def replace_one(self, *args, **kwargs):
        return super().replace_one(*args, **kwargs)

    @sync_to_async
    def _find(self, *args, **kwargs):
        return super()._find(*args, **kwargs)

    @sync_to_async
    def find_one(self, *args, **kwargs):
        return super().find_one(*args, **kwargs)

    @sync_to_async
    def _aggregate(self, *args, **kwargs):
        return super()._aggregate(*args, **kwargs)

    @sync_to_async
    def _bulk_operation(self, *args, **kwargs):
        return super()._bulk_operation(*args, **kwargs)

    @sync_to_async
    def _find_with_replacement_or_with_update(self, *args, **kwargs):
        return super()._find_with_replacement_or_with_update(*args, **kwargs)

    @sync_to_async
    def count(self, *args, **kwargs):
        return super().count(*args, **kwargs)

    @no_type_check
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
        data = await self._find(  # type: ignore
            logical_query, skip_rows, limit_rows, session, sort_fields, sort, **query
        )
        return QuerySet(self._mongo_model, data)

    @no_type_check
    async def count_documents(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        # type: ignore
        return await self.count(logical_query, session, **query)

    @no_type_check
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

    @no_type_check
    async def simple_aggregate(self, *args, **kwargs):
        return await self._aggregate(*args, **kwargs)

    @no_type_check
    async def aggregate_sum(self, agg_field: str, **query) -> int:
        result = await self._aggregate(aggregation=Sum(agg_field), **query)
        return result.get(f'{agg_field}__sum', 0)

    @no_type_check
    async def aggregate_max(self, agg_field: str, **query) -> int:
        result = await self._aggregate(aggregation=Max(agg_field), **query)
        return result.get(f'{agg_field}__max', 0)

    @no_type_check
    async def aggregate_min(self, agg_field: str, **query) -> int:
        result = await self._aggregate(aggregation=Min(agg_field), **query)
        return result.get(f'{agg_field}__min', 0)

    @no_type_check
    async def aggregate_avg(self, agg_field: str, **query) -> int:
        result = await self._aggregate(aggregation=Avg(agg_field), **query)
        return result.get(f'{agg_field}__avg', 0)

    @no_type_check
    async def bulk_create(
        self,
        models: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> int:  # type: ignore
        if batch_size is None or batch_size <= 0:
            batch_size = 30000
        result = 0
        for data in chunk_by_length(models, batch_size):
            # type: ignore
            result += await self.insert_many(data, session=session)
        return result

    @no_type_check
    async def bulk_update(
        self,
        models: List,
        updated_fields: List,
        batch_size: Optional[int] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not updated_fields:
            raise MongoValidationError('updated_fields cannot be empty')
        await self._bulk_operation(
            models,
            updated_fields=updated_fields,
            batch_size=batch_size,
            session=session,
        )

    @no_type_check
    async def bulk_update_or_create(
        self,
        models: List,
        query_fields: List,
        batch_size: Optional[int] = 10000,
        session: Optional[ClientSession] = None,
    ) -> None:
        if not query_fields:
            raise MongoValidationError('query_fields cannot be empty')
        await self._bulk_operation(
            models,
            query_fields=query_fields,
            batch_size=batch_size,
            upsert=True,
            session=session,
        )

    @no_type_check
    async def get(
        self,
        logical_query: Union[Query, LogicalCombination, None] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        **query,
    ) -> Any:  # type: ignore
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

    @no_type_check
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

    @no_type_check
    async def update_or_create(self, **query) -> Tuple:
        defaults = query.pop('defaults', {})
        obj = await self.find_one(**query)
        if obj is not None:
            created = False
            for field, value in defaults.items():
                setattr(obj, field, value)

        else:
            created = True
            obj = self._mongo_model(**{**query, **defaults})  # type: ignore
        await obj.save_async()
        return obj, created

    @no_type_check
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
        count = await self.count(
            session=session,
            logical_query=logical_query,
            **query,
        )
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

    @no_type_check
    async def find_one_and_update(
        self,
        session: Optional[ClientSession] = None,
        projection_fields: Optional[List] = None,
        **query,
    ) -> Optional[Union['MongoModel', Dict]]:
        filter_, set_values = self._prepare_update_data(**query)
        if projection_fields:
            projection = {field: 1 for field in projection_fields}
            data = await self.__query(
                'find_one_and_update',
                filter_,
                {'$set': set_values},
                session=session,
                projection=projection,
                return_document=ReturnDocument.AFTER,
            )
            return data
        data = await self.__query(
            'find_one_and_update',
            filter_,
            {'$set': set_values},
            session=session,
            return_document=ReturnDocument.AFTER,
        )
        if data:
            return self._mongo_model.model_validate(data)
        return None
