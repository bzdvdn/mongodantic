from abc import ABC
from typing import TYPE_CHECKING, Any, Type

from .exceptions import MongoValidationError

if TYPE_CHECKING:
    from .models import MongoModel


__all__ = ('Sum', 'Avg', 'Min', 'Count', 'Max')


class BasicDefaultAggregation(ABC):
    """Abstract class for Aggregation"""

    _operation: Any = None

    def __init__(self, field: str):
        self.field = field

    @property
    def operation(self) -> str:
        if not self._operation:
            raise NotImplementedError('implement _operation')
        return self._operation

    def validate(self, mongo_model: Type['MongoModel']) -> None:
        """Validate field name

        Args:
            mongo_model (Type[MongoModel]): mongo model class

        Raises:
            MongoValidationError: if invalid field
        """
        # Get the actual model class if we received an instance
        model_class = mongo_model if isinstance(
            mongo_model, type) else mongo_model.__class__

        if self.field not in model_class.model_fields and self.field != '_id':
            raise MongoValidationError(
                f'invalid field "{self.field}" for this model, field must be one of {list(model_class.model_fields.keys())}'
            )

    def _aggregate_query(self, mongo_model: 'MongoModel') -> dict:
        self.validate(mongo_model)  # Pass the model instance directly
        query = {
            f'{self.field}__{self.operation}': {f'${self.operation}': f'${self.field}'}
        }
        return query


class Sum(BasicDefaultAggregation):
    _operation: Any = 'sum'


class Max(BasicDefaultAggregation):
    _operation: Any = 'max'


class Min(BasicDefaultAggregation):
    _operation: Any = 'min'


class Avg(BasicDefaultAggregation):
    _operation: Any = 'avg'


class Count(BasicDefaultAggregation):
    _operation: Any = 'count'

    def _aggregate_query(self, mongo_model: 'MongoModel') -> dict:
        self.validate(mongo_model)
        query = {
            "_id": f'${self.field}' if self.field != '_id' else None,
            f'count': {f'$sum': 1},
        }
        return query
