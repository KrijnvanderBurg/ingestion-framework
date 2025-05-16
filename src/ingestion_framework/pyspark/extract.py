"""
TODO
"""

from abc import ABC
from typing import Any, Generic

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.extract import (
    ExtractAbstract,
    ExtractContextAbstract,
    ExtractFormat,
    ExtractMethod,
    ExtractModelFilePyspark,
    ExtractModelPyspark,
    ExtractModelT,
)
from ingestion_framework.types import DataFrameT
from ingestion_framework.utils.spark_handler import SparkHandler


class ExtractPyspark(ExtractAbstract[ExtractModelPyspark, DataFramePyspark]):
    """
    Concrete implementation for PySpark DataFrame extraction.
    """

    # extract_model_concrete = ExtractModelPyspark

    def extract(self) -> None:
        """
        Main extraction method.
        """
        SparkHandler().add_configs(options=self.model.options)

        if self.model.method == ExtractMethod.BATCH:
            self.data_registry[self.model.name] = self._extract_batch()
        elif self.model.method == ExtractMethod.STREAMING:
            self.data_registry[self.model.name] = self._extract_streaming()
        else:
            raise ValueError(f"Extraction method {self.model.method} is not supported for Pyspark.")


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


class ExtractContextPyspark(ExtractContextAbstract):
    """
    TODO
    """

    strategy: dict[ExtractFormat, type[ExtractAbstract[Any, Any]]] = {
        ExtractFormat.PARQUET: ExtractFilePyspark,
        ExtractFormat.JSON: ExtractFilePyspark,
        ExtractFormat.CSV: ExtractFilePyspark,
    }
