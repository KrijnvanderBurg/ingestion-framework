"""
PySpark implementation for data extraction operations.

This module provides concrete implementations for extracting data using PySpark.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton
from ingestion_framework.utils.schema import SchemaFilepathHandler
from ingestion_framework.utils.spark import SparkHandler

NAME: Final[str] = "name"
METHOD: Final[str] = "method"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA: Final[str] = "schema"
OPTIONS: Final[str] = "options"

ExtractModelT = TypeVar("ExtractModelT", bound="ExtractModel")


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


class ExtractModel(ABC):
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


class Extract(Generic[ExtractModelT]):
    """
    Abstract base class for data extraction operations.

    This class defines the interface for all extraction implementations,
    supporting both batch and streaming extractions.
    """

    extract_model_concrete: type[ExtractModelT]

    def __init__(self, model: ExtractModelT) -> None:
        """
        Initialize the extraction operation.

        Args:
            model: Configuration model for the extraction
        """
        self.model = model
        self.data_registry = DataFrameRegistry()

    @property
    def model(self) -> ExtractModelT:
        """Get the extraction model configuration."""
        return self._model

    @model.setter
    def model(self, value: ExtractModelT) -> None:
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
        model = cls.extract_model_concrete.from_confeti(confeti=confeti)
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


class ExtractModelFile(ExtractModel):
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
            schema = SchemaFilepathHandler.parse(schema=confeti[SCHEMA])
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


@ExtractRegistry.register(ExtractFormat.PARQUET)
@ExtractRegistry.register(ExtractFormat.JSON)
@ExtractRegistry.register(ExtractFormat.CSV)
class ExtractFile(Extract[ExtractModelFile]):
    """
    Abstract class for file extraction.
    """

    extract_model_concrete = ExtractModelFile
    _spark_handler: SparkHandler = SparkHandler()

    def _extract_batch(self) -> DataFrame:
        """
        Read from file in batch mode using PySpark.
        """

        return self._spark_handler.session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFrame:
        """
        Read from file in streaming mode using PySpark.
        """
        return self._spark_handler.session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )


class ExtractContext:
    """
    Abstract context class for creating and managing extraction strategies.

    This class implements the Strategy pattern for data extraction, allowing
    different extraction implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, confeti: dict[str, type[Extract]]) -> type[Extract]:
        """
        Create an appropriate extract class based on the format specified in the configuration.

        This factory method uses the ExtractRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            confeti: Configuration dictionary that must include a 'data_format' key
                compatible with the ExtractFormat enum

        Returns:
            The concrete extraction class for the specified format

        Raises:
            NotImplementedError: If the specified extract format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        try:
            extract_format = ExtractFormat(confeti[DATA_FORMAT])
            return ExtractRegistry.get(extract_format)
        except KeyError as e:
            format_name = confeti.get(DATA_FORMAT, "<missing>")
            raise NotImplementedError(f"Extract format {format_name} is not supported.") from e
