"""
Recipe for filtering dataframe rows based on a condition."""

from typing import Any

from pyspark.sql import functions as F

from ingestion_framework.types import DataFramePysparkRegistry
from ingestion_framework2.pyspark.transforms.recipes.base import RecipePyspark
from ingestion_framework2.pyspark.transforms.registry import register_recipe


@register_recipe("filter_data")
class FilterDataRecipe(RecipePyspark):
    """Recipe for filtering dataframe rows."""

    def __init__(self, column: str, operator: str, value: Any) -> None:
        """
        Initialize with filter parameters.

        Args:
            column (str): Column to filter on
            operator (str): Comparison operator ('>', '<', '>=', '<=', '==', '!=')
            value (Any): Value to compare against
        """
        self.column = column
        self.operator = operator
        self.value = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> "FilterDataRecipe":
        """
        Create a recipe instance from configuration.

        Args:
            confeti (dict[str, Any]): Configuration dictionary

        Returns:
            FilterDataRecipe: Configured recipe instance
        """
        arguments = confeti.get("arguments", {})
        column = arguments.get("column", "")
        operator = arguments.get("operator", "==")
        value = arguments.get("value")

        return cls(column=column, operator=operator, value=value)

    def callable_(self, dataframe_registry: DataFramePysparkRegistry, dataframe_name: str) -> None:
        """
        Filter rows in the dataframe.

        Args:
            dataframe_registry (DataFramePysparkRegistry): Registry containing dataframes
            dataframe_name (str): Name of the dataframe to transform
        """
        df = dataframe_registry[dataframe_name]

        # Apply filter based on operator
        col_ref = F.col(self.column)
        if self.operator == ">":
            df_transformed = df.filter(col_ref > self.value)
        elif self.operator == "<":
            df_transformed = df.filter(col_ref < self.value)
        elif self.operator == ">=":
            df_transformed = df.filter(col_ref >= self.value)
        elif self.operator == "<=":
            df_transformed = df.filter(col_ref <= self.value)
        elif self.operator == "==":
            df_transformed = df.filter(col_ref == self.value)
        elif self.operator == "!=":
            df_transformed = df.filter(col_ref != self.value)
        else:
            # If unsupported operator, return original dataframe
            df_transformed = df

        # Update the registry with transformed dataframe
        dataframe_registry[dataframe_name] = df_transformed
