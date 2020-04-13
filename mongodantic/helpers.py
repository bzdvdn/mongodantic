from typing import List, Any, Dict, Tuple, Union, Optional, Callable
from pydantic.main import ModelMetaclass
from bson import ObjectId
from pymongo import UpdateOne
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure, \
    WriteConcernError

from .exceptions import MongoConnectionError


class ExtraQueryMapper(object):
    def __init__(self, field_name: str):
        self.field_name = field_name

    def extra_query(self, extra_methods: List, values) -> Dict:
        if self.field_name == '_id':
            values = [ObjectId(v) for v in values] if isinstance(values, list) else ObjectId(values)
        if extra_methods:
            query = {self.field_name: {}}
            for extra_method in extra_methods:
                if extra_method == 'in':
                    extra_method = 'in_'
                elif extra_method == 'inc':
                    return self.inc(value)
                query[self.field_name].update(getattr(self, extra_method)(values))
            return query
        return {}

    def in_(self, list_values: List):
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        return {"$in": list_values}

    def regex(self, regex_value: str):
        return {"$regex": regex_value}

    def regex_ne(self, regex_value: str):
        return {"not": {"$regex": regex_value}}

    def ne(self, value: Any):
        return {"$ne": value}

    def startswith(self, value: str):
        return {"$regex": f"^{value}"}

    def not_startswith(self, value: str):
        return {"not": {"$regex": f"^{value}"}}

    def endswith(self, value: str):
        return {"$regex": f"{value}$"}

    def not_endswith(self, value: str):
        return {"not": {"$regex": f"{value}$"}}

    def nin(self, list_values: List):
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        return {"$nin": list_values}

    def exists(self, boolean_value: bool):
        return {"$exists": boolean_value}

    def type(self, bson_type):
        return {"$type": bson_type}

    def gte(self, value: Any):
        return {"$gte": value}

    def lte(self, value: Any):
        return {"$lte": value}

    def gt(self, value: Any):
        return {"$gt": value}

    def lt(self, value: Any):
        return {"$lt": value}

    def inc(self, value: int):
        if isinstance(value, int):
            return {'$inc': {self.field_name: value}}
        raise ValueError('value must be integer')

    def range(self, range_values: Union[List, Tuple]):
        if len(range_values) != 2:
            raise ValueError("range must have 2 params")
        from_ = range_values[0]
        to_ = range_values[1]
        return {"$gte": from_, "$lte": to_}


def chunk_by_length(items: List, step: int):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(items), step):
        yield items[i: i + step]


def bulk_query_generator(requests: List, updated_fields: Optional[List] = None,
                         query_fields: Optional[List] = None, upsert=False) -> List:
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
        except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure, WriteConcernError) as e:
            raise MongoConnectionError(str(e))
    return wrapper


def generate_lookup_project_params(main_model: ModelMetaclass, reference_model: ModelMetaclass, as_: str) -> Dict:
    project_param = {f: 1 for f in main_model.__fields__}
    project_param['_id'] = 1
    project_param.update(
        {f'{as_}.{f}': 1 for f in ['_id'] + list(reference_model.__fields__.keys())})
    return project_param
