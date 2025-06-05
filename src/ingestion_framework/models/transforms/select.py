"""
Column transform function.
"""

from abc import ABC
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.core.transform import FunctionModel
from ingestion_framework.exceptions import DictKeyError

# Import these locally to avoid circular imports
from ingestion_framework.models.transform import ArgsModel

COLUMNS: Final[str] = "columns"


class SelectFunctionModelArgs(ArgsModel):
    """The arguments for  DataFrame Select functions."""

    def __init__(self, columns: list[Column]) -> None:
        self.columns = columns

    @property
    def columns(self) -> list[Column]:
        return self._columns

    @columns.setter
    def columns(self, value: list[Column]) -> None:
        self._columns = value

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create Args object from a JSON dictionary.

        Args:
            dict_ (dict[str, Any]): The JSON dictionary.

        Returns:
            SelectFunctionModel.Args: The Args object created from the JSON dictionary.
        """
        try:
            columns = []
            for col_name in dict_[COLUMNS]:
                columns.append(f.col(col_name))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(columns=columns)


class SelectFunctionModel(FunctionModel[SelectFunctionModelArgs]):
    """A concrete implementation of DataFrame Select functions using ."""

    args_concrete = SelectFunctionModelArgs

    class Args(ArgsModel, ABC):
        """An abstract base class for arguments of Select functions."""
