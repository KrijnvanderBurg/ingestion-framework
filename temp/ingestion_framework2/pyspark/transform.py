"""
PySpark implementation for data transformation operations.

This module provides concrete implementations for transforming data using PySpark.
"""

from abc import ABC

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework2.pyspark.function import FunctionPyspark
from ingestion_framework2.pyspark.functions.select import SelectFunctionPyspark
from ingestion_framework2.transform import (
    TransformAbstract,
    TransformModelAbstract,
    TransformRegistry,
)


class TransformModelPyspark(TransformModelAbstract):
    """
    Modelification for PySpark data transformation.

    Examples:
        >>> df = spark.createDataFrame(data=[("Alice", 27), ("Bob", 32),], schema=["name", "age"])
        >>> dict = {"function": "cast", "arguments": {"columns": {"age": "StringType",}}}
        >>> transform = TransformFunction.from_dict(dict=dict[str, Any])
        >>> df = df.transform(func=transform).printSchema()
        root
        |-- name: string (nullable = true)
        |-- age: string (nullable = true)
    """


@TransformRegistry.register("default")
class TransformPyspark(TransformAbstract[TransformModelPyspark, FunctionPyspark, DataFramePyspark], ABC):
    """
    Concrete implementation for PySpark DataFrame transformation.

    This class provides PySpark-specific functionality for transforming data.
    """

    load_model_concrete = TransformModelPyspark
    SUPPORTED_FUNCTIONS: dict[str, type[FunctionPyspark]] = {
        "select": SelectFunctionPyspark,
    }
