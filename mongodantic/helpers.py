from re import compile
from typing import (
    List,
    Any,
    Dict,
    Tuple,
    Union,
    Optional,
    Callable,
    TYPE_CHECKING,
    Type,
)
from pydantic import BaseModel as BasePydanticModel
from bson import ObjectId
from pymongo import UpdateOne
from pymongo.errors import (
    ServerSelectionTimeoutError,
    AutoReconnect,
    NetworkTimeout,
    ConnectionFailure,
    WriteConcernError,
)

from .exceptions import MongoConnectionError, ValidationError
from .types import ObjectIdStr

if TYPE_CHECKING:
    from .models import BaseModel

__all__ = (
    'ExtraQueryMapper',
    'chunk_by_length',
    'bulk_query_generator',
    'generate_operator_for_multiply_aggregations',
    'cached_classproperty',
)


class cached_classproperty(classmethod):
    def __init__(self, fget):
        self.obj = {}
        self.fget = fget

    def __get__(self, owner, cls):
        if cls in self.obj:
            return self.obj[cls]
        self.obj[cls] = self.fget(cls)
        return self.obj[cls]


class classproperty(classmethod):
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


def _validate_value(cls: Type['BaseModel'], field_name: str, value: Any) -> Any:
    field = cls.__fields__.get(field_name)
    if not field:
        raise AttributeError(f'invalid field - {field_name}')
    error_ = None
    if isinstance(field, ObjectIdStr):
        try:
            value = field.validate(value)
        except ValueError as e:
            error_ = e
    else:
        value, error_ = field.validate(value, {}, loc=field.alias, cls=cls)
    if error_:
        raise ValidationError([error_], type(value))
    return value


class ExtraQueryMapper(object):
    def __init__(self, model: Type['BaseModel'], field_name: str):
        self.field_name = field_name
        self.model = model

    def extra_query(self, extra_methods: List, values) -> Dict:
        if self.field_name == '_id':
            values = (
                [ObjectId(v) for v in values]
                if isinstance(values, list)
                else ObjectId(values)
            )
        if extra_methods:
            query: Dict = {self.field_name: {}}
            for extra_method in extra_methods:
                if extra_method == 'in':
                    extra_method = 'in_'
                elif extra_method == 'inc':
                    return self.inc(values)
                elif extra_method == 'unset':
                    return self.unset(values)
                query[self.field_name].update(getattr(self, extra_method)(values))
            return query
        return {}

    def in_(self, list_values: List) -> dict:
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        try:
            return {
                "$in": [
                    _validate_value(self.model, self.field_name, v) for v in list_values
                ]
            }
        except ValidationError:
            return {"$in": list_values}

    def regex(self, regex_value: str) -> dict:
        return {"$regex": regex_value}

    def iregex(self, regex_value: str) -> dict:
        return {"$regex": regex_value, "$options": "i"}

    def regex_ne(self, regex_value: str) -> dict:
        return {"$not": compile(regex_value)}

    def ne(self, value: Any) -> dict:
        return {"$ne": _validate_value(self.model, self.field_name, value)}

    def startswith(self, value: str) -> dict:
        return {"$regex": f"^{value}"}

    def istartswith(self, value: str) -> dict:
        return {"$regex": f"^{value}", "$options": "i"}

    def not_startswith(self, value: str) -> dict:
        return {"$not": compile(f"^{value}")}

    def endswith(self, value: str) -> dict:
        return {"$regex": f"{value}$"}

    def iendswith(self, value: str) -> dict:
        return {"$regex": f"{value}$", "$options": "i"}

    def not_endswith(self, value: str) -> dict:
        return {"$not": compile(f"{value}$")}

    def nin(self, list_values: List) -> dict:
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        try:
            return {
                "$nin": [
                    _validate_value(self.model, self.field_name, v) for v in list_values
                ]
            }
        except ValidationError:
            return {"$nin": list_values}

    def exists(self, boolean_value: bool) -> dict:
        if not isinstance(boolean_value, bool):
            raise TypeError("boolean_value must be a bool type")
        return {"$exists": boolean_value}

    def type(self, bson_type) -> dict:
        return {"$type": bson_type}

    def search(self, search_text: str) -> dict:
        return {'$search': search_text}

    def all(self, query: Any) -> dict:
        return {'$all': query}

    def unset(self, value: Any) -> dict:
        return {"$unset": {self.field_name: value}}

    def gte(self, value: Any) -> dict:
        return {"$gte": _validate_value(self.model, self.field_name, value)}

    def lte(self, value: Any) -> dict:
        return {"$lte": _validate_value(self.model, self.field_name, value)}

    def gt(self, value: Any) -> dict:
        return {"$gt": _validate_value(self.model, self.field_name, value)}

    def lt(self, value: Any) -> dict:
        return {"$lt": _validate_value(self.model, self.field_name, value)}

    def inc(self, value: int) -> dict:
        if isinstance(value, int):
            return {'$inc': {self.field_name: value}}
        raise ValueError('value must be integer')

    def range(self, range_values: Union[List, Tuple]) -> dict:
        if len(range_values) != 2:
            raise ValueError("range must have 2 params")
        from_ = range_values[0]
        to_ = range_values[1]
        return {
            "$gte": _validate_value(self.model, self.field_name, from_),
            "$lte": _validate_value(self.model, self.field_name, to_),
        }

    @cached_classproperty
    def methods(cls) -> list:
        methods = []
        for f in cls.__dict__:
            if f == 'in_':
                methods.append('in')
            elif not f.startswith('__') and f != 'extra_query':
                methods.append(f)
        return methods


def chunk_by_length(items: List, step: int):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(items), step):
        yield items[i : i + step]


def bulk_query_generator(
    requests: List,
    updated_fields: Optional[List] = None,
    query_fields: Optional[List] = None,
    upsert=False,
) -> List:
    data = []
    if updated_fields:
        for obj in requests:
            query = obj.data
            query['_id'] = ObjectId(query['_id'])
            update = {}
            for field in updated_fields:
                value = query.pop(field)
                update.update({field: value})
            data.append(UpdateOne(query, {'$set': update}, upsert=upsert))
    elif query_fields:
        for obj in requests:
            query = {}
            update = {}
            for field, value in obj.data.items():
                if field not in query_fields:
                    update.update({field: value})
                else:
                    query.update({field: value})
            data.append(UpdateOne(query, {'$set': update}, upsert=upsert))
    return data


def handle_and_convert_connection_errors(func: Callable) -> Any:
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (
            ServerSelectionTimeoutError,
            AutoReconnect,
            NetworkTimeout,
            ConnectionFailure,
            WriteConcernError,
        ) as e:
            raise MongoConnectionError(str(e))

    return wrapper


def generate_lookup_project_params(
    main_model: BasePydanticModel, reference_models: Dict[str, BasePydanticModel]
) -> Dict:
    project_param = {f: 1 for f in main_model.__fields__}
    project_param['_id'] = 1
    for as_, reference_model in reference_models.items():
        project_param.update(
            {f'{as_}.{f}': 1 for f in ['_id'] + list(reference_model.__fields__.keys())}
        )
    return project_param


def generate_name_field(name: Union[dict, str, None] = None):
    if isinstance(name, dict):
        return '|'.join(str(v) for v in name.values())
    return name


def sort_validation(
    sort: Optional[int] = None, sort_fields: Union[list, tuple, None] = None
) -> Tuple[Any, ...]:
    if sort is not None:
        if sort not in (1, -1):
            raise ValueError(f'invalid sort value must be 1 or -1 not {sort}')
        if not sort_fields:
            sort_fields = ('_id',)
    return sort, sort_fields


def group_by_aggregate_generation(group_by: Any) -> Any:
    if isinstance(group_by, (list, tuple)):
        return {
            g if '.' not in g else g.split('.')[-1]: f'${g}' if '$' not in g else g
            for g in group_by
        }
    if '.' in group_by:
        name = group_by.split('.')[-1]
        return {name: f'${group_by}'}
    return f'${group_by}' if not '$' in group_by else group_by
