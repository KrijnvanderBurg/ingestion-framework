"""
Column transform function.
"""

from abc import ABC
from collections.abc import Callable
from typing import Any, Final, Self

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.recipes.base import (
    ArgsAbstract,
    ArgsT,
    RecipeAbstract,
    RecipeModelAbstract,
    RecipeModelT,
    RecipePyspark,
)
from ingestion_framework.types import RegistrySingleton

COLUMNS: Final[str] = "columns"


class SelectColumnsRecipeModelAbstract(RecipeModelAbstract[ArgsT], ABC):
    """An abstract base class for DataFrame Select recipes."""

    class Args(ArgsAbstract, ABC):
        """An abstract base class for arguments of Select recipes."""


class SelectColumnsRecipeModelPysparkArgs(SelectColumnsRecipeModelAbstract.Args):
    """The arguments for PySpark DataFrame Select recipes."""

    def __init__(self, columns: list[Column]) -> None:
        self.columns = columns

    @property
    def columns(self) -> list[Column]:
        return self._columns

    @columns.setter
    def columns(self, value: list[Column]) -> None:
        self._columns = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create Args object from a JSON dictionary.

        Args:
            confeti (dict[str, Any]): The JSON dictionary.

        Returns:
            SelectColumnsRecipeModelPyspark.Args: The Args object created from the JSON dictionary.
        """
        try:
            columns = []
            for col_name in confeti[COLUMNS]:
                columns.append(f.col(col_name))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(columns=columns)


class SelectColumnsRecipeModelPyspark(SelectColumnsRecipeModelAbstract[SelectColumnsRecipeModelPysparkArgs]):
    """A concrete implementation of DataFrame Select recipes using PySpark."""

    args_concrete = SelectColumnsRecipeModelPysparkArgs


class SelectColumnsRecipeAbstract(RecipeAbstract[RecipeModelT], ABC):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """


class SelectColumnsRecipePyspark(SelectColumnsRecipeAbstract[SelectColumnsRecipeModelPyspark], RecipePyspark):
    """
    Encapsulates column transformation logic for PySpark DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_confeti(confeti: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """

    model_concrete = SelectColumnsRecipeModelPyspark

    def transform(self) -> Callable:
        """
        Selects columns with aliases.

        Returns:
            (Callable): Recipe for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'select_columns' function:

            ```
            {"recipe": "select_columns", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def __f(dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(
                *self.model.arguments.columns
            )

        return __f
