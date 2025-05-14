"""
PySpark file-based data loading implementation.

This module provides concrete implementations for loading data to file-based destinations
using PySpark. It supports various file formats such as Parquet, JSON, and CSV, in both
batch and streaming modes.
"""

from abc import ABC
from typing import Generic

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

from ingestion_framework.load.base import LoadAbstract, LoadModelFilePyspark, LoadModelT, LoadPyspark
from ingestion_framework.load.registry import load_registry
from ingestion_framework.types import DataFrameT, StreamingQueryT


class LoadFileAbstract(
    LoadAbstract[LoadModelT, DataFrameT, StreamingQueryT], Generic[LoadModelT, DataFrameT, StreamingQueryT], ABC
):
    """
    Abstract class for file loadion.
    """


class LoadFilePyspark(LoadFileAbstract[LoadModelFilePyspark, DataFramePyspark, StreamingQueryPyspark], LoadPyspark):
    """
    Concrete implementation for file-based data loading using PySpark.

    This class provides the implementation for writing data to file-based destinations
    using PySpark DataFrameWriter and DataStreamWriter APIs. It supports both batch
    and streaming write operations with various file formats.

    Attributes:
        load_model_concrete: The concrete model class used for configuration.
    """

    load_model_concrete = LoadModelFilePyspark

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        self.data_registry[self.model.name].write.save(
            path=self.model.location,
            format=self.model.data_format.value,
            mode=self.model.mode.value,
            **self.model.options,
        )

    def _load_streaming(self) -> StreamingQueryPyspark:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        return self.data_registry[self.model.name].writeStream.start(
            path=self.model.location,
            format=self.model.data_format.value,
            outputMode=self.model.mode.value,
            **self.model.options,
        )


# Register load formats
load_registry.register("parquet")(LoadFilePyspark)
load_registry.register("json")(LoadFilePyspark)
load_registry.register("csv")(LoadFilePyspark)
