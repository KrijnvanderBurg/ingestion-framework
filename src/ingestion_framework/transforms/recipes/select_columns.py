"""
Column transform function with simplified recipe registry pattern.
"""

from typing import Any, Final

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.recipes.registry import Recipe, recipe_registry
from ingestion_framework.types import RegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)

# Constants
COLUMNS: Final[str] = "columns"
ARGUMENTS: Final[str] = "arguments"


# Make sure this decorator is executed at import time
@recipe_registry.register("select_columns")
class SelectColumnsRecipePyspark(Recipe):
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
        logger.info(f"SelectColumnsRecipePyspark initialized with columns: {[col.name for col in columns]}")

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
        logger.info(f"Creating SelectColumnsRecipePyspark from confeti: {confeti}")
        try:
            arguments = confeti[ARGUMENTS]
            columns = []
            for col_name in arguments[COLUMNS]:
                columns.append(f.col(col_name))
                logger.info(f"Added column: {col_name}")
        except KeyError as e:
            logger.error(f"KeyError in SelectColumnsRecipePyspark.from_confeti: {e.args[0]}")
            raise DictKeyError(key=e.args[0], dict_=confeti if e.args[0] == ARGUMENTS else arguments) from e

        return cls(columns=columns)

    def callable_(self, dataframe_registry: RegistrySingleton, dataframe_name: str) -> None:
        """
        Apply the column selection transformation to a dataframe.
        """
        logger.info(f"Executing select_columns on dataframe: {dataframe_name}")
        logger.info(f"Columns before: {dataframe_registry[dataframe_name].columns}")

        # Get only column names for better logging
        column_names = [col.name for col in self.columns]
        logger.info(f"Selecting columns: {column_names}")

        # Apply the transformation
        dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(*self.columns)

        # Log the result
        logger.info(f"Columns after: {dataframe_registry[dataframe_name].columns}")


# Print a message to confirm this module is being imported
print("SelectColumnsRecipePyspark registered with recipe_registry")
