"""
TODO
"""

from abc import ABC
from typing import Generic

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.extract.base import ExtractAbstract, ExtractModelFilePyspark, ExtractModelT, ExtractPyspark
from ingestion_framework.types import DataFrameT
from ingestion_framework.utils.spark_handler import SparkHandler


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract class for file extraction.
    """


class ExtractFilePyspark(ExtractFileAbstract[ExtractModelFilePyspark, DataFramePyspark], ExtractPyspark):
    """
    Concrete class for file extraction using PySpark DataFrame.
    """

    extract_model_concrete = ExtractModelFilePyspark

    def _extract_batch(self) -> DataFramePyspark:
        """
        Read from file in batch mode using PySpark.
        """
        return SparkHandler().session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFramePyspark:
        """
        Read from file in streaming mode using PySpark.
        """
        return SparkHandler().session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )
