"""
PySpark select transformation implementation.

This module provides a PySpark implementation of the select transformation function,
which allows selecting specific columns from DataFrames with various selection options.
"""

from abc import ABC
from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame as DataFramePyspark

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.functions.base import (
    ArgsAbstract,
    ArgsT,
    FunctionAbstract,
    FunctionModelAbstract,
    FunctionModelT,
    FunctionPyspark,
)
from ingestion_framework.types import RegistrySingleton

COLUMNS: Final[str] = "columns"


class SelectFunctionModelAbstract(FunctionModelAbstract[ArgsT], ABC):
    """
    Abstract base class for DataFrame Select function models.

    This class represents the configuration for column selection transformations.
    """

    class Args(ArgsAbstract, ABC):
        """
        Abstract base class for arguments of Select functions.

        This class defines the interface for arguments used by select transformation functions.
        """


class SelectFunctionModelPysparkArgs(SelectFunctionModelAbstract.Args):
    """
    Arguments for PySpark DataFrame Select functions.

    This class implements the Args interface for PySpark select transformations.

    Attributes:
        columns (list[Column]): The list of columns to select.
    """

    def __init__(self, columns: list[Column]) -> None:
        """
        Initialize arguments for a select transformation.

        Args:
            columns (list[Column]): The list of columns to select.
        """
        self.columns = columns

    @property
    def columns(self) -> list[Column]:
        """
        Get the list of columns to select.

        Returns:
            list[Column]: The list of columns.
        """
        return self._columns

    @columns.setter
    def columns(self, value: list[Column]) -> None:
        """
        Set the list of columns to select.

        Args:
            value (list[Column]): The list of columns to set.
        """
        self._columns = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            SelectFunctionModelPysparkArgs: The Args object created from the configuration.

        Raises:
            DictKeyError: If a required key is missing from the configuration.

        Examples:
            >>> confeti = {"columns": ["name", "age", "address"]}
            >>> args = SelectFunctionModelPysparkArgs.from_confeti(confeti)
        """
        try:
            columns = []
            for col_name in confeti[COLUMNS]:
                columns.append(f.col(col_name))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(columns=columns)


class SelectFunctionModelPyspark(SelectFunctionModelAbstract[SelectFunctionModelPysparkArgs]):
    """
    PySpark-specific implementation of Select function model.

    This class implements the SelectFunctionModelAbstract for PySpark DataFrames.
    """

    args_concrete = SelectFunctionModelPysparkArgs


class SelectFunctionAbstract(FunctionAbstract[FunctionModelT], ABC):
    """
    Abstract base class for Select transformation functions.

    This class represents a transformation function for selecting columns from a DataFrame.
    """


class SelectFunctionPyspark(SelectFunctionAbstract[SelectFunctionModelPyspark], FunctionPyspark):
    """
    PySpark-specific implementation of Select transformation function.

    This class implements the SelectFunctionAbstract for PySpark DataFrames, providing
    functionality to select specific columns.

    Attributes:
        model (SelectFunctionModelPyspark): The Select function model.
    """

    model_concrete = SelectFunctionModelPyspark

    def transform(self) -> Callable:
        """
        Create a callable that selects columns from a DataFrame.

        Returns:
            Callable: A function that selects columns from the DataFrame in the registry.

        Examples:
            Consider the following DataFrame schema:
            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            |-- address: string (nullable = true)
            ```

            Applying the select function with columns ["name", "age"]:
            ```
            {"function": "select", "arguments": {"columns": ["name", "age"]}}
            ```

            The resulting DataFrame schema will be:
            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```
        """

        def __f(dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
            """
            Select columns from the DataFrame in the registry.

            Args:
                dataframe_registry (RegistrySingleton): The registry containing DataFrames.
                dataframe_name (str): The name of the DataFrame in the registry.
            #"""
            df: DataFramePyspark = dataframe_registry[dataframe_name]

            df = df.select(*self.model.arguments.columns)

            dataframe_registry[dataframe_name] = df

        return __f
