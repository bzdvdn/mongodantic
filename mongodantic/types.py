from bson import ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel


class ObjectIdStr(str):
    """Field for validate string like ObjectId"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, ObjectId):
            return str(v)
        else:
            try:
                ObjectId(str(v))
            except InvalidId:
                raise ValueError('Not a valid ObjectId')
            return v
