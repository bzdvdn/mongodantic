from typing import Optional
from pydantic.main import ModelMetaclass


class LookupCombination(object):
    def __init__(self, lookup):
        self.children = []
        for node in lookup:
            if isinstance(node, LookupCombination):
                self.children.extend(node.children)
            else:
                self.children.append(node)

    def __repr__(self):
        return " AND ".join([repr(node) for node in self.children])


class Lookup(object):
    def __init__(
        self,
        from_collection: ModelMetaclass,
        local_field: str,
        foreign_field: str,
        as_: Optional[str] = None,
        unwind_path: Optional[str] = None,
        preserve_null_and_empty_arrays: bool = False,
    ):
        self.from_collection = from_collection
        self.local_field = local_field
        self.foreign_field = foreign_field
        self._as = as_ if as_ else self.from_collection.__name__.lower()
        self.unwind_path = unwind_path
        self.preserve_null_and_empty_arrays = preserve_null_and_empty_arrays

    def to_query(self) -> list:
        query = [
            {
                '$lookup': {
                    'localField': self._validate_local_field(),
                    'from': self.from_collection,
                    'foreignField': self.foreign_field,
                    'as': self.as_,
                }
            }
        ]
        if self.unwind_path:
            query.append(
                {
                    '$unwind': {
                        'path': self.unwind_path,
                        'preserveNullAndEmptyArrays': self.preserve_null_and_empty_arrays,
                    }
                }
            )
        return query

    def _validate_local_field(self) -> str:
        if (
            self.local_field not in self.from_collection.__fields___
            and self.local_field != '_id'
        ):
            raise AttributeError('invalid local_field')
        return self.local_field

    def _combine(self, other) -> LookupCombination:
        return LookupCombination([self, other])

    def __and__(self, other):
        return self._combine(other)

    def __repr__(self):
        return f'Lookup(from_collection={self.from_collection.__name__.lower()}, local_field={self.local_field}, foreign_field={self.foreign_field}, as={self.as_}'
