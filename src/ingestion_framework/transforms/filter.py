"""
Filter transform function for filtering data based on conditions.
"""

from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import Column
from pyspark.sql import functions as f

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transform import Args, Function, FunctionModel
from ingestion_framework.transforms import TransformFunctionRegistry
from ingestion_framework.types import DataFrameRegistry

COLUMN: Final[str] = "column"
OPERATOR: Final[str] = "operator"
VALUE: Final[str] = "value"


class FilterFunctionModelArgs(Args):
    """The arguments for DataFrame Filter functions."""

    def __init__(self, column: str, operator: str, value: Any) -> None:
        self.column = column
        self.operator = operator
        self.value = value

    @property
    def column(self) -> str:
        return self._column

    @column.setter
    def column(self, value: str) -> None:
        self._column = value

    @property
    def operator(self) -> str:
        return self._operator

    @operator.setter
    def operator(self, value: str) -> None:
        self._operator = value

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a configuration dictionary.

        Args:
            confeti: The configuration dictionary.

        Returns:
            FilterFunctionModelArgs: The Args object created from the configuration dictionary.
        """
        try:
            column = confeti[COLUMN]
            operator = confeti[OPERATOR]
            value = confeti[VALUE]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(column=column, operator=operator, value=value)


class FilterFunctionModel(FunctionModel[FilterFunctionModelArgs]):
    """A concrete implementation of DataFrame Filter functions."""

    args_concrete = FilterFunctionModelArgs


@TransformFunctionRegistry.register("filter")
class FilterFunction(Function[FilterFunctionModel]):
    """
    Encapsulates filter transformation logic for DataFrames.

    Attributes:
        model: The FilterModel object containing the filtering information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create FilterFunction object from configuration.
        transform() -> Callable: Filters the DataFrame based on the specified condition.
    """

    model_concrete = FilterFunctionModel

    def transform(self) -> Callable:
        """
        Creates a filter function based on the specified condition.

        Returns:
            Callable: Function that applies the filter to a DataFrame.
        """

        def apply_filter_condition(column: Column) -> Column:
            """Apply the appropriate filter condition based on the operator."""
            op = self.model.arguments.operator.lower()
            value = self.model.arguments.value

            if op == "equals" or op == "eq" or op == "==":
                return column == value
            elif op == "not_equals" or op == "neq" or op == "!=":
                return column != value
            elif op == "greater_than" or op == "gt" or op == ">":
                return column > value
            elif op == "greater_than_or_equal" or op == "gte" or op == ">=":
                return column >= value
            elif op == "less_than" or op == "lt" or op == "<":
                return column < value
            elif op == "less_than_or_equal" or op == "lte" or op == "<=":
                return column <= value
            elif op == "contains":
                return column.contains(value)
            elif op == "starts_with":
                return column.startswith(value)
            elif op == "ends_with":
                return column.endswith(value)
            elif op == "is_null":
                return column.isNull()
            elif op == "is_not_null":
                return column.isNotNull()
            else:
                raise ValueError(f"Unsupported operator: {op}")

        def __f(dataframe_registry: DataFrameRegistry, dataframe_name: str) -> None:
            column_name = self.model.arguments.column
            filter_condition = apply_filter_condition(f.col(column_name))
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].filter(filter_condition)

        return __f
