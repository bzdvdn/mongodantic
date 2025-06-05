from re import compile, IGNORECASE
from time import sleep
from types import GeneratorType
from typing import (
    Generator,
    List,
    Any,
    Dict,
    Tuple,
    Union,
    Optional,
    Callable,
    TYPE_CHECKING,
    Type,
    Annotated,
)
from bson import ObjectId
from pymongo import UpdateOne
from pymongo.errors import (
    ServerSelectionTimeoutError,
    AutoReconnect,
    NetworkTimeout,
    ConnectionFailure,
)
from pydantic_core import PydanticCustomError

from .exceptions import MongoConnectionError, MongoValidationError
from .types import ObjectIdStr

if TYPE_CHECKING:
    from .models import MongoModel

__all__ = (
    'handle_and_convert_connection_errors',
    'ExtraQueryMapper',
    'chunk_by_length',
    'bulk_query_generator',
    'cached_classproperty',
    'group_by_aggregate_generation',
    'classproperty',
    'sort_validation',
    'generate_name_field',
    '_validate_value',
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
        return self.fget(cls)  # type: ignore

    def getter(self, method):
        self.fget = method
        return self


def _validate_value(cls, field_name: str, value: Any) -> Any:
    """Validate value for field

    Args:
        cls: MongoModel class
        field_name: field name
        value: value to validate

    Returns:
        Any: validated value

    Raises:
        PydanticCustomError: If field doesn't exist or value type is invalid
    """
    # Skip validation for Pydantic internal fields
    if field_name.startswith('model_'):
        return value

    # Special handling for _id field
    if field_name == '_id':
        if isinstance(value, str):
            return ObjectId(value)
        return value

    # Skip type validation for array query values
    if '__' in field_name:
        return value

    field = cls.model_fields.get(field_name)  # type: ignore
    if field is None:
        raise PydanticCustomError(
            "field_not_found",
            f"Field '{field_name}' not found in model",
            dict(field_name=field_name)
        )

    # Get the actual type from Annotated if present
    field_type = field.annotation
    if hasattr(field_type, '__origin__') and field_type.__origin__ is Annotated:
        field_type = field_type.__args__[0]

    if field_type == ObjectIdStr:
        if isinstance(value, str):
            return ObjectId(value)
        return value

    # Handle list types
    if hasattr(field_type, '__origin__') and field_type.__origin__ is list:
        # For direct list assignments, validate the value is a list
        if not isinstance(value, list):
            raise PydanticCustomError(
                "type_error",
                "Value must be of type list",
                dict(type="list")
            )
        # Get the inner type of the list
        inner_type = field_type.__args__[0]
        try:
            # For list fields, we don't validate the inner type
            if inner_type is Any or inner_type is object:
                return value
            return [_validate_value(cls, field_name, item) for item in value]
        except TypeError:
            return value

    # Validate type
    try:
        if not isinstance(value, field_type):
            raise PydanticCustomError(
                "type_error",
                f"Value must be of type {field_type.__name__}",
                dict(type=field_type.__name__)
            )
    except TypeError:
        # Skip type validation for complex types
        pass

    return value


class ExtraQueryMapper(object):
    """extra mapper for __ queries like find(_id__in=[], name__regex='123')"""

    def __init__(self, model: Type['MongoModel'], field_name: str):
        self.field_name = field_name
        self.model = model

    def extra_query(self, extra_methods: List, values: Any) -> Dict:
        if self.field_name == '_id':
            values = (
                [ObjectId(v) for v in values]
                if isinstance(values, list)
                else ObjectId(values)
            )
        if extra_methods:
            # Handle special cases first
            if extra_methods[-1] == 'set':
                # For updates like config__username__set
                field_path = '.'.join([self.field_name] + extra_methods[:-1])
                return {field_path: values}
            elif extra_methods[-1] == 'unset':
                return self.unset(values)
            elif extra_methods[-1] == 'inc':
                return self.inc(values)

            # For regular queries, build the field path
            field_path = '.'.join([self.field_name] + extra_methods)
            return {field_path: values}
        return {}

    def in_(self, list_values: List) -> dict:
        # Get the base field name without the __in suffix
        base_field = self.field_name.split('__')[0]
        # Get the field type
        field = self.model.model_fields.get(base_field)
        if field is None:
            return {"$in": list_values if isinstance(list_values, list) else [list_values]}

        # Get the actual type from Annotated if present
        field_type = field.annotation
        if hasattr(field_type, '__origin__') and field_type.__origin__ is Annotated:
            field_type = field_type.__args__[0]

        # For list fields, use $in operator directly
        if hasattr(field_type, '__origin__') and field_type.__origin__ is list:
            # Convert single value to list if needed
            values = list_values if isinstance(
                list_values, list) else [list_values]
            # For array fields, we use $in operator directly
            return {"$in": values}

        # For other fields, validate each value
        try:
            # Convert tuple to list if needed
            if isinstance(list_values, tuple):
                raise TypeError("values must be a list type")
            values = list_values if isinstance(
                list_values, list) else [list_values]
            # Pass the full field name to _validate_value to ensure array query validation is skipped
            return {
                "$in": [
                    _validate_value(self.model, f"{base_field}__in", v) for v in values
                ]
            }
        except MongoValidationError:
            return {"$in": list_values if isinstance(list_values, list) else [list_values]}

    def regex(self, regex_value: str) -> dict:
        return {"$regex": regex_value}

    def iregex(self, regex_value: str) -> dict:
        # For case-insensitive regex, we need to escape special characters
        # and use the 'i' option
        escaped_value = regex_value.replace(
            '\\', '\\\\').replace('$', '\\$').replace('.', '\\.')
        return {"$regex": escaped_value, "$options": "i"}

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

    def not_istartswith(self, value: str) -> dict:
        return {"$not": compile(f"^{value}", IGNORECASE)}

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
            # Get the base field name without the __nin suffix
            base_field = self.field_name.split('__')[0]
            # For array fields, we don't validate the values
            if hasattr(self.model.model_fields.get(base_field), 'annotation') and \
               hasattr(self.model.model_fields[base_field].annotation, '__origin__') and \
               self.model.model_fields[base_field].annotation.__origin__ is list:
                return {"$nin": list_values}
            return {
                "$nin": [
                    _validate_value(self.model, base_field, v) for v in list_values
                ]
            }
        except MongoValidationError:
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
        return tuple(methods)  # Convert to tuple to support membership testing


def chunk_by_length(items: List, step: int) -> Generator:
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(items), step):
        yield items[i: i + step]


def bulk_query_generator(
    requests: List,
    updated_fields: Optional[List] = None,
    query_fields: Optional[List] = None,
    upsert=False,
) -> List:
    """helper for generate bulk query"""
    queries = []
    for request in requests:
        query = {}
        if query_fields:
            for field in query_fields:
                if hasattr(request, field):
                    query[field] = getattr(request, field)
        else:
            query = {"_id": request._id}

        update = {}
        if updated_fields:
            for field in updated_fields:
                if hasattr(request, field):
                    update[field] = getattr(request, field)
        else:
            update = request.model_dump(exclude={"_id"})

        queries.append(
            UpdateOne(
                query,
                {"$set": update},
                upsert=upsert
            )
        )
    return queries


def handle_and_convert_connection_errors(func: Callable) -> Any:
    """decorator for handle connection errors and raise MongoConnectionError

    Args:
        func (Callable):any query to mongo

    Returns:
        Any: data
    """

    def generator_wrapper(generator):
        yield from generator

    def main_wrapper(*args, **kwargs):
        counter = 1
        while True:
            try:
                result = func(*args, **kwargs)
                if isinstance(result, GeneratorType):
                    result = generator_wrapper(result)
                return result
            except (
                AutoReconnect,
                ServerSelectionTimeoutError,
                NetworkTimeout,
                ConnectionFailure,
            ) as e:
                counter += 1
                if counter > 5:
                    raise MongoConnectionError(str(e))
                sleep(counter)

    return main_wrapper


def generate_name_field(name: Union[dict, str, None] = None) -> Optional[str]:
    if isinstance(name, dict):
        return '|'.join(str(v) for v in name.values())
    return name


def sort_validation(
    sort: Optional[Union[int, tuple]] = None, sort_fields: Union[list, tuple, None] = None
) -> Tuple[Any, Any]:
    """Validate sort fields"""
    if sort is not None:
        if isinstance(sort, tuple):
            if len(sort) != 2:
                raise ValueError("sort tuple must have exactly 2 elements")
            field, direction = sort
            if direction not in (1, -1):
                raise ValueError("invalid sort value must be 1 or -1")
            return (sort,)
        elif isinstance(sort, int):
            if sort not in (1, -1):
                raise ValueError(
                    f"invalid sort value must be 1 or -1 not {sort}")
            if not sort_fields:
                sort_fields = ('_id',)
            return sort, sort_fields
        else:
            raise ValueError("sort must be an integer or tuple")

    if sort_fields is None:
        return None, None

    if isinstance(sort_fields, tuple):
        if len(sort_fields) != 2:
            raise ValueError("sort tuple must have exactly 2 elements")
        field, direction = sort_fields
        if direction not in (1, -1):
            raise ValueError("invalid sort value must be 1 or -1")
        return (sort_fields,)

    if isinstance(sort_fields, list):
        return None, tuple(sort_fields)

    raise ValueError("sort_fields must be a tuple or list")


def group_by_aggregate_generation(
    group_by: Union[str, list, tuple]
) -> Union[str, dict]:
    """group by parametr generation helper"""

    if isinstance(group_by, (list, tuple)):
        return {
            g if '.' not in g else g.split('.')[-1]: f'${g}' if '$' not in g else g
            for g in group_by
        }
    if '.' in group_by:
        name = group_by.split('.')[-1]
        return {name: f'${group_by}'}
    return f'${group_by}' if not '$' in group_by else group_by
