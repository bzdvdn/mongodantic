from pydantic.fields import ModelField
from bson import ObjectId
from pymongo import UpdateOne


class ExtraQueryMapper(object):
    def __init__(self, field_name):
        self.field_name = field_name

    def extra_query(self, extra_methods: list, values) -> dict:
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

    def in_(self, list_values):
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        return {"$in": list_values}

    def regex(self, regex_value):
        return {"$regex": regex_value}

    def regex_ne(self, regex_value):
        return {"not": {"$regex": regex_value}}

    def ne(self, value):
        return {"$ne": value}

    def startswith(self, value):
        return {"$regex": f"^{value}"}

    def endswith(self, value):
        return {"$regex": f"{value}$"}

    def nin(self, list_values):
        if not isinstance(list_values, list):
            raise TypeError("values must be a list type")
        return {"$nin": list_values}

    def exists(self, boolean_value):
        return {"$exists": boolean_value}

    def type(self, bson_type):
        return {"$type": bson_type}

    def gte(self, value):
        return {"$gte": value}

    def lte(self, value):
        return {"$lte": value}

    def gt(self, value):
        return {"$gt": value}

    def lt(self, value):
        return {"$lt": value}

    def inc(self, value):
        if isinstance(value, int):
            return {'$inc': {self.field_name: value}}
        raise ValueError('value must be integer')

    def range(self, range_values):
        if len(range_values) != 2:
            raise ValueError("range must have 2 params")
        from_ = range_values[0]
        to_ = range_values[1]
        return {"$gte": from_, "$lte": to_}


def chunk_by_length(items: list, step: int):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(items), step):
        yield items[i: i + step]


def bulk_update_query_generator(requests: list, updated_fields: list) -> list:
    data = []
    for obj in requests:
        query = obj.data
        query['_id'] = ObjectId(query['_id'])
        update = {}
        for field in updated_fields:
            value = query.pop(field)
            update.update({field: value})
        data.append(UpdateOne(query, {'$set': update}))
    return data
