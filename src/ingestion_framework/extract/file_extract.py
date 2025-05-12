"""
PySpark file-based data extraction implementation.

This module provides concrete implementations for extracting data from file-based sources
using PySpark. It supports various file formats such as Parquet, JSON, and CSV, in both
batch and streaming modes.
"""

from abc import ABC
from typing import Generic

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.extract.base import ExtractAbstract, ExtractModelFilePyspark, ExtractModelT, ExtractPyspark
from ingestion_framework.types import DataFrameT
from ingestion_framework.utils.spark_handler import SparkHandler


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract base class for file-based data extraction.

    This class extends the general extraction interface specifically for file-based sources,
    providing a foundation for implementations that read data from files in various formats.
    """


class ExtractFilePyspark(ExtractFileAbstract[ExtractModelFilePyspark, DataFramePyspark], ExtractPyspark):
    """
    Concrete implementation for file-based data extraction using PySpark.

    This class provides the implementation for reading data from file-based sources
    using PySpark DataFrameReader and DataStreamReader APIs. It supports both batch
    and streaming read operations with various file formats.

    Attributes:
        extract_model_concrete: The concrete model class used for configuration.
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
