"""
Column transform function with simplified recipe registry pattern.
"""

from collections.abc import Callable
from typing import Any, Final

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.recipes.base import RecipePyspark, recipe_registry
from ingestion_framework.types import DataFrameRegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)

# Constants
COLUMNS: Final[str] = "columns"
ARGUMENTS: Final[str] = "arguments"


# Make sure this decorator is executed at import time
@recipe_registry.register("select_columns")
class SelectColumnsRecipePyspark(RecipePyspark):
    """
    Recipe for selecting columns from a DataFrame.

    Simplified implementation that handles PySpark column selection.
    """

    def __init__(self, columns: list[Column]) -> None:
        """
        Initialize the select columns recipe.

        Args:
            columns: List of columns to select
        """
        self.columns = columns
        # Call transform to set callable_
        self.callable_ = self.transform()

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> "SelectColumnsRecipePyspark":
        """
        Create recipe object from a JSON dictionary.

        Args:
            confeti: The JSON configuration dictionary

        Returns:
            SelectColumnsRecipePyspark: An initialized recipe

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        try:
            arguments = confeti[ARGUMENTS]
            columns = []
            for col_name in arguments[COLUMNS]:
                columns.append(f.col(col_name))
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(columns=columns)

    def transform(self) -> Callable:
        """
        Define the transformation to select columns from a dataframe.

        Returns:
            Callable: A function that selects the specified columns from a dataframe
        """

        def select_columns(dataframe_registry: DataFrameRegistrySingleton, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(*self.columns)

        return select_columns
