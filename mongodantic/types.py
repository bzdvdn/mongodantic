from bson import ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel


class ObjectIdStr(str):
    """Field for validate string like ObjectId"""

    type_ = ObjectId
    required = False
    default = None
    validate_always = False
    alias = ''

    # def __init__(self, *args):
    #     if kwargs:
    #         self.alias = args[0]
    #     super().__init__()

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(
        cls, v,
    ):
        if isinstance(v, ObjectId):
            return v
        else:
            try:
                return ObjectId(str(v))
            except InvalidId:
                raise ValueError(f"invalid ObjectId - {v}")
