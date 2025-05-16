# """
# Ingestion datetime recipe.

# This recipe adds a timestamp column to track when data was ingested into the system.
# """

# from collections.abc import Callable
# from typing import Any

# from pyspark.sql import DataFrame as DataFramePyspark
# from pyspark.sql import functions as F

# from ingestion_framework.pyspark.transforms.recipes.base import RecipePyspark
# from ingestion_framework.types import DataFramePysparkRegistry


# class AddIngestionDatetimeRecipe(RecipePyspark):
#     """
#     Recipe for adding an ingestion datetime column to a dataframe.

#     Adds a timestamp column with the current datetime to indicate when the data was ingested.

#     Arguments:
#         column_name (str): The name of the timestamp column to add. Defaults to "ingestion_datetime".
#     """

#     def __init__(self, arguments: dict[str, Any]) -> None:
#         """
#         Initialize the ingestion datetime recipe.

#         Args:
#             arguments (dict[str, Any] | None, optional): Configuration for the recipe.
#         """
#         super().__init__(arguments)
#         self.column_name = arguments.get("column_name", "ingestion_datetime")

#     def transform(self) -> Callable:
#         """
#         Create a callable for adding the ingestion datetime.

#         Returns:
#             Callable: A callable that adds an ingestion timestamp to a DataFrame.
#         """
#         column_name = self.column_name

#         def __f(dataframe_registry: DataFramePysparkRegistry, dataframe_name: str) -> None:
#             """
#             Add an ingestion datetime column to the dataframe.

#             Args:
#                 dataframe_registry (DataFramePysparkRegistry): The registry containing DataFrames.
#                 dataframe_name (str): The name of the DataFrame in the registry.
#             """
#             df: DataFramePyspark = dataframe_registry[dataframe_name]
#             df = df.withColumn(column_name, F.current_timestamp())
#             dataframe_registry[dataframe_name] = df

#         return __f
