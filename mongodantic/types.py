from typing import TypeVar, Generic, Type, Union, Any, Annotated
from bson import ObjectId, DBRef
from bson.errors import InvalidId
from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema
from pydantic import BeforeValidator, PlainValidator


T = TypeVar("T")


def validate_object_id(v: Any) -> str:
    if isinstance(v, ObjectId):
        return str(v)
    try:
        return str(ObjectId(str(v)))
    except InvalidId:
        raise ValueError(f"invalid ObjectId - {v}")


ObjectIdStr = Annotated[str, BeforeValidator(validate_object_id)]


class RefrerenceType(Generic[T]):
    def __init__(self, ref: DBRef, model_class: Type[T]):
        self.ref = ref
        self.model_class = model_class

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetJsonSchemaHandler) -> CoreSchema:
        return core_schema.union_schema([
            core_schema.is_instance_schema(DBRef),
            core_schema.is_instance_schema(source_type.__args__[0]),
            core_schema.chain_schema([
                core_schema.plain_validator_function(cls.validate),
                core_schema.json_or_python_schema(
                    json_schema=handler(source_type.__args__[0]),
                    python_schema=handler(source_type.__args__[0])
                )
            ])
        ])

    @classmethod
    def validate(cls, v: Union[DBRef, T], info: Any) -> Any:
        model_class = info.sub_fields[0].type_  # type: ignore
        if isinstance(v, DBRef):
            return cls(ref=v, model_class=model_class).to_ref()
        if isinstance(v, model_class):
            ref = DBRef(model_class.set_collection_name(), model_class._id)
            return cls(ref=ref, model_class=model_class).to_ref()
        return model_class.validate(v)

    def to_ref(self):
        return self.ref

    @classmethod
    async def find_one_async(cls):
        pass
