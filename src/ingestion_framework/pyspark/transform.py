"""
TODO
"""

from abc import ABC
from typing import Any

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.pyspark.function import FunctionPyspark
from ingestion_framework.pyspark.functions.select import SelectFunctionPyspark
from ingestion_framework.transform import TransformAbstract, TransformModelAbstract


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


class TransformPyspark(TransformAbstract[TransformModelPyspark, FunctionPyspark, DataFramePyspark], ABC):
    """
    Concrete implementation for PySpark DataFrame transformion.
    """

    load_model_concrete = TransformModelPyspark
    SUPPORTED_FUNCTIONS: dict[str, Any] = {
        "select": SelectFunctionPyspark,
    }
