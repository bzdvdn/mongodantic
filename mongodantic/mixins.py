import abc
from typing import Dict
from pydantic import BaseModel as BasePyDanticModel

from .db import DBConnection


class DBMixin(BasePyDanticModel, abc.ABC):
    class _Meta:
        _database = DBConnection().database
