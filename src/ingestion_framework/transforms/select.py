"""
Column transform function.
"""

from abc import ABC
from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.core.transform import Function, FunctionModel, TransformFunctionRegistry
from ingestion_framework.exceptions import DictKeyError

# Import these locally to avoid circular imports
from ingestion_framework.models.transform import ArgsModel
from ingestion_framework.types import DataFrameRegistry

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


@TransformFunctionRegistry.register("select")
class SelectFunction(Function[SelectFunctionModel]):
    """
    Encapsulates column transformation logic for DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_dict(dict_: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """

    model_concrete = SelectFunctionModel

    def transform(self) -> Callable:
        """
        Selects columns with aliases.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the dict_ 'select_with_alias' function:

            ```
            {"function": "select_with_alias", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def __f(dataframe_registry: DataFrameRegistry, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(
                *self.model.arguments.columns
            )

        return __f
