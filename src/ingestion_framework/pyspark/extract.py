"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, Self

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.types import StructType

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.extract import (
    DATA_FORMAT,
    LOCATION,
    METHOD,
    NAME,
    OPTIONS,
    SCHEMA,
    ExtractAbstract,
    ExtractContextAbstract,
    ExtractFormat,
    ExtractMethod,
    ExtractModelAbstract,
    ExtractModelFileAbstract,
    ExtractModelT,
    ExtractRegistry,
)
from ingestion_framework.types import DataFrameT
from ingestion_framework.utils.schema_handler import SchemaHandlerPyspark
from ingestion_framework.utils.spark_handler import SparkHandler


class ExtractModelPyspark(ExtractModelAbstract, ABC):
    """
    PySpark implementation of extract model.

    This model configures extraction operations specific to PySpark,
    including schema and options handling.
    """

    def __init__(
        self,
        name: str,
        method: ExtractMethod,
        options: dict[str, str],
        schema: StructType | None = None,
    ) -> None:
        """
        Initialize ExtractModelPyspark with the specified parameters.

        Args:
            name: Identifier for this extraction operation
            method: Method of extraction (batch or streaming)
            options: PySpark reader options as key-value pairs
            schema: Optional schema definition for the data structure
        """
        super().__init__(
            name=name,
            method=method,
        )
        self.options: dict[str, str] = options
        self.schema: StructType | None = schema

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class ExtractPyspark(ExtractAbstract[ExtractModelPyspark, DataFramePyspark]):
    """
    Concrete implementation for PySpark DataFrame extraction.
    """

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


class ExtractModelFilePyspark(ExtractModelFileAbstract, ExtractModelPyspark):
    """
    Model for file extraction using PySpark.

    This model configures extraction operations for reading files with PySpark,
    including format, location, and schema information.
    """

    def __init__(
        self,
        name: str,
        method: ExtractMethod,
        data_format: ExtractFormat,
        location: str,
        options: dict[str, str],
        schema: StructType | None = None,
    ) -> None:
        """
        Initialize ExtractModelFilePyspark with the specified parameters.

        Args:
            name: Identifier for this extraction operation
            method: Method of extraction (batch or streaming)
            data_format: Format of the files to extract (parquet, json, csv)
            location: URI where the files are located
            options: PySpark reader options as key-value pairs
            schema: Optional schema definition for the data structure
        """
        super().__init__(
            name=name,
            method=method,
            options=options,
            schema=schema,
        )
        self.data_format = data_format
        self.location = location

    @property
    def data_format(self) -> ExtractFormat:
        """Get the format of the files to extract."""
        return self._data_format

    @data_format.setter
    def data_format(self, value: ExtractFormat) -> None:
        """Set the format of the files to extract."""
        self._data_format = value

    @property
    def location(self) -> str:
        """Get the location URI of the files to extract."""
        return self._location

    @location.setter
    def location(self, value: str) -> None:
        """Set the location URI of the files to extract."""
        self._location = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an ExtractModelFilePyspark object from a configuration dictionary.

        Args:
            confeti: The configuration dictionary containing extraction parameters

        Returns:
            An initialized extraction model for file-based sources

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        try:
            name = confeti[NAME]
            method = ExtractMethod(confeti[METHOD])
            data_format = ExtractFormat(confeti[DATA_FORMAT])
            location = confeti[LOCATION]
            options = confeti[OPTIONS]
            schema = SchemaHandlerPyspark.schema_factory(schema=confeti[SCHEMA])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(
            name=name,
            method=method,
            data_format=data_format,
            location=location,
            options=options,
            schema=schema,
        )


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract class for file extraction.
    """


@ExtractRegistry.register(ExtractFormat.PARQUET)
@ExtractRegistry.register(ExtractFormat.JSON)
@ExtractRegistry.register(ExtractFormat.CSV)
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
    PySpark implementation of extraction context.

    This class provides factory methods for creating PySpark extractors.
    """
