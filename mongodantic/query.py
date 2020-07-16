from pymongo.collection import Collection
from pymongo.client_session import ClientSession
from pydantic.main import ModelMetaclass

from .types import ObjectIdStr
from .exceptions import (
    NotDeclaredField,
    InvalidArgument,
    ValidationError,
    MongoIndexError,
    MongoConnectionError,
    InvalidArgsParams,
)
from .helpers import (
    ExtraQueryMapper,
    chunk_by_length,
    bulk_query_generator,
    generate_lookup_project_params,
    generate_operator_for_multiply_aggregations,
)
from .queryset import QuerySet
from .logical import LogicalCombination, Query


class Query(obejct):
    def __init__(self, collection: Collection, fields: dict, exclude_fields: tuple):
        self._collection = collection
        self.fields = fields
        self.exclude_fields = exclude_fields

    def get_collection(self) -> Collection:
        return self._collection

    def __validate_value(self, field_name: str, value: Any) -> Any:
        field = self.__fields__[field_name]
        if isinstance(field, ObjectIdStr):
            try:
                value = field.validate(value)
            except ValueError as e:
                error_ = e
        else:
            value, error_ = field.validate(value, {}, loc=field.alias, self=self)
        if error_:
            raise ValidationError([error_], type(value))
        return value

    def _check_query_args(
        self, logical_query: Union[Query, LogicalCombination, None] = None
    ) -> Dict:
        if not isinstance(logical_query, (LogicalCombination, Query)):
            raise InvalidArgsParams()
        return logical_query.to_query(self)

    def _query(
        self,
        method_name: str,
        query_params: Union[List, Dict, str],
        set_values: Optional[Dict] = None,
        session: Optional[ClientSession] = None,
        counter: int = 1,
        logical: bool = False,
        **kwargs,
    ) -> Any:
        inner_query_params = query_params
        if isinstance(query_params, dict) and not logical:
            query_params = self._validate_query_data(query_params)
        collection = self._get_collection()
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
        except (
            NetworkTimeout,
            AutoReconnect,
            ConnectionFailure,
            WriteConcernError,
            ServerSelectionTimeoutError,
        ) as description:
            self._reconnect()
            if counter >= 5:
                raise MongoConnectionError(str(description))
            counter += 1
            return self._query(
                method_name=method_name,
                query_params=inner_query_params,
                set_values=set_values,
                session=session,
                counter=counter,
                logical=logical,
                **kwargs,
            )
