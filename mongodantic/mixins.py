import abc
from typing import Dict
from pydantic import BaseModel as BasePyDanticModel


class DBMixin(BasePyDanticModel, abc.ABC):
    """Base class for Pydantic mixins"""

    _doc: Dict = None

    def _update_model_from__doc(self):
        """
        Update model fields from _doc dictionary
        (projection of a document from DB)
        """
        for name, field in self.__fields__.items():
            value = self._doc.get(name)
            if issubclass(field.type_, BaseModel) and isinstance(value, dict):
                value = field.type_.parse_obj(value)
            setattr(self, name, value)
        return self
