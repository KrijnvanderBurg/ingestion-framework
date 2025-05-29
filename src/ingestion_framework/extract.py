"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
"""

from abc import abstractmethod
from enum import Enum
from typing import Any, Final, Self

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton
from ingestion_framework.utils.spark import SparkHandler

NAME: Final[str] = "name"
METHOD: Final[str] = "method"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA: Final[str] = "schema"
OPTIONS: Final[str] = "options"


class ExtractRegistry(RegistryDecorator, metaclass=Singleton):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """


class ExtractMethod(Enum):
    """
    Types of extract modes.
    """

    BATCH = "batch"
    STREAMING = "streaming"


class ExtractFormat(Enum):
    """Types of input and structures for extract."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class ExtractModel:
    """
    Abstract base class for extract operation models.

    This class defines the configuration model for data extraction operations,
    specifying the name and method for the extraction.
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
        self.name = name
        self.method = method
        self.options: dict[str, str] = options
        self.schema: StructType | None = schema

    @property
    def name(self) -> str:
        """Get the name of the extraction operation."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set the name of the extraction operation."""
        self._name = value

    @property
    def method(self) -> ExtractMethod:
        """Get the extraction method (batch or streaming)."""
        return self._method

    @method.setter
    def method(self, value: ExtractMethod) -> None:
        """Set the extraction method (batch or streaming)."""
        self._method = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an extraction model from a configuration dictionary.

        Args:
            confeti: Configuration dictionary containing extraction parameters

        Returns:
            An initialized extraction model

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """


class Extract:
    """
    Abstract base class for data extraction operations.

    This class defines the interface for all extraction implementations,
    supporting both batch and streaming extractions.
    """

    def __init__(self, model: ExtractModel) -> None:
        """
        Initialize the extraction operation.

        Args:
            model: Configuration model for the extraction
        """
        self.model = model
        self.data_registry = DataFrameRegistry()

    @property
    def model(self) -> ExtractModel:
        """Get the extraction model configuration."""
        return self._model

    @model.setter
    def model(self, value: ExtractModel) -> None:
        """Set the extraction model configuration."""
        self._model = value

    @property
    def data_registry(self) -> DataFrameRegistry:
        """Get the data registry for storing extraction results."""
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFrameRegistry) -> None:
        """Set the data registry for storing extraction results."""
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an extraction instance from a configuration dictionary.

        Args:
            confeti: Configuration dictionary containing extraction specifications

        Returns:
            An initialized extraction instance

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        model = ExtractModel.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _extract_batch(self) -> DataFrame:
        """
        Extract data in batch mode.

        Returns:
            The extracted data as a DataFrame
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrame:
        """
        Extract data in streaming mode.

        Returns:
            The extracted data as a streaming DataFrame
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


@ExtractRegistry.register(ExtractFormat.PARQUET)
@ExtractRegistry.register(ExtractFormat.JSON)
@ExtractRegistry.register(ExtractFormat.CSV)
class ExtractFile(Extract):
    """
    Abstract class for file extraction.
    """

    def _extract_batch(self) -> DataFrame:
        """
        Read from file in batch mode using PySpark.
        """
        return SparkHandler().session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFrame:
        """
        Read from file in streaming mode using PySpark.
        """
        return SparkHandler().session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )
