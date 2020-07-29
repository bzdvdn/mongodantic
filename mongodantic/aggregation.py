from typing import Optional
from pydantic.main import ModelMetaclass

from .exceptions import ValidationError
from .helpers import generate_lookup_project_params


class LookupCombination(object):
    def __init__(self, lookup: list):
        self.children = []
        for node in lookup:
            if node in self.children:
                continue
            elif isinstance(node, LookupCombination):
                self.children.extend(node.children)
            else:
                self.children.append(node)

    def __repr__(self):
        return " AND ".join([repr(node) for node in self.children])

    def accept(
        self, main_model: ModelMetaclass, project: Optional[dict] = None
    ) -> tuple:
        accepted_lookup = []
        reference_models = {}
        for node in self.children:
            accepted_lookup.extend(node.to_query(main_model))
            reference_models[node.as_] = node.from_collection
        if not project:
            project = generate_lookup_project_params(main_model, reference_models)
        accepted_lookup.append({'$project': project})
        return accepted_lookup, reference_models


class Lookup(object):
    def __init__(
        self,
        from_collection: ModelMetaclass,
        local_field: str,
        foreign_field: str,
        as_: Optional[str] = None,
        with_unwind: bool = False,
        preserve_null_and_empty_arrays: bool = False,
    ):
        self.from_collection = from_collection
        self.local_field = local_field
        self.foreign_field = foreign_field
        self.as_ = as_ if as_ else self.from_collection.collection_name
        self.with_unwind = with_unwind
        self.preserve_null_and_empty_arrays = preserve_null_and_empty_arrays

    def to_query(self, main_model: ModelMetaclass) -> list:
        query = [
            {
                '$lookup': {
                    'localField': self._validate_local_field(main_model),
                    'from': self.from_collection.collection_name,
                    'foreignField': self.foreign_field,
                    'as': self.as_,
                }
            }
        ]
        if self.with_unwind:
            query.append(
                {
                    '$unwind': {
                        'path': f'${self.as_}',
                        'preserveNullAndEmptyArrays': self.preserve_null_and_empty_arrays,
                    }
                }
            )
        return query

    def _validate_local_field(self, main_model: ModelMetaclass) -> str:
        if (
            self.local_field
            not in set(
                self.from_collection.__fields__.keys() | main_model.__fields__.keys()
            )
            and self.local_field != '_id'
        ):
            raise AttributeError('invalid local_field')
        return self.local_field

    def _combine(self, other) -> LookupCombination:
        return LookupCombination([self, other])

    def accept(
        self, main_model: ModelMetaclass, project: Optional[dict] = None
    ) -> tuple:
        return LookupCombination([self]).accept(main_model, project)

    def __and__(self, other):
        return self._combine(other)

    def __repr__(self):
        return f'Lookup(from_collection={self.from_collection.__name__.lower()}, local_field={self.local_field}, foreign_field={self.foreign_field}, as={self.as_}'

