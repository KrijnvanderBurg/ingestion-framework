"""
Extract interface and implementations for various data formats.

This module provides abstract classes and implementations for data extraction
from various sources and formats.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.types import (
    DataFramePysparkRegistry,
    DataFrameT,
    DecoratorRegistrySingleton,
)

NAME: Final[str] = "name"
METHOD: Final[str] = "method"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA: Final[str] = "schema"
OPTIONS: Final[str] = "options"


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


class ExtractModelAbstract(ABC):
    """
    Abstract base class for extract operation models.

    This class defines the configuration model for data extraction operations,
    specifying the name and method for the extraction.
    """

    def __init__(self, name: str, method: ExtractMethod) -> None:
        """
        Initialize the extraction model with basic parameters.

        Args:
            name: Identifier for this extraction operation
            method: Method of extraction to use (batch or streaming)
        """
        self.name = name
        self.method = method

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


class ExtractModelFileAbstract(ExtractModelAbstract, ABC):
    """
    Abstract base class for file-based extraction models.

    This class serves as a marker interface for all extraction models
    that read data from file-based sources.
    """


ExtractModelT = TypeVar("ExtractModelT", bound=ExtractModelAbstract)


class ExtractAbstract(Generic[ExtractModelT, DataFrameT], ABC):
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
        self.data_registry = DataFramePysparkRegistry()

    @property
    def model(self) -> ExtractModelT:
        """Get the extraction model configuration."""
        return self._model

    @model.setter
    def model(self, value: ExtractModelT) -> None:
        """Set the extraction model configuration."""
        self._model = value

    @property
    def data_registry(self) -> DataFramePysparkRegistry:
        """Get the data registry for storing extraction results."""
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFramePysparkRegistry) -> None:
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
    def _extract_batch(self) -> DataFrameT:
        """
        Extract data in batch mode.

        Returns:
            The extracted data as a DataFrame
        """

    @abstractmethod
    def _extract_streaming(self) -> DataFrameT:
        """
        Extract data in streaming mode.

        Returns:
            The extracted data as a streaming DataFrame
        """

    @abstractmethod
    def extract(self) -> None:
        """
        Execute the extraction operation based on the configured method.

        This method should determine whether to use batch or streaming extraction
        based on the model configuration and place the result in the data registry.
        """


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract class for file extraction.
    """


class ExtractContextAbstract(ABC):
    """
    Abstract context class for creating and managing extraction strategies.

    This class implements the Strategy pattern for data extraction, allowing
    different extraction implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[ExtractAbstract]:
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


# Create a specific registry for Extract implementations
class ExtractRegistry(DecoratorRegistrySingleton[ExtractFormat, ExtractAbstract]):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """
