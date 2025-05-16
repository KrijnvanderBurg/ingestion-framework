"""
Extract interface and implementations for various data formats.

This module provides abstract classes and implementations for data extraction
from various sources and formats.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql.types import StructType

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.types import (
    DataFramePysparkRegistry,
    DataFrameT,
    DecoratorRegistry,
)
from ingestion_framework.utils.schema_handler import SchemaHandlerPyspark

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
    ExtractModel class.

    Args:
        name (str): ID of the extract modelification.
        method (str): method of extract mode.
        data_format (str): format of the extract.
        location (str): uri that identifies from where to extract data in the modelified format.
    """

    def __init__(self, name: str, method: ExtractMethod) -> None:
        """
        Initialize ExtractModelAbstract with the modelified parameters.
        """
        self.name = name
        self.method = method

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def method(self) -> ExtractMethod:
        return self._method

    @method.setter
    def method(self, value: ExtractMethod) -> None:
        self._method = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class ExtractModelPyspark(ExtractModelAbstract, ABC):
    """
    ExtractModel pyspark class.

    Args:
        name (str): ID of the extract modelification.
        method (ExtractMethod): ReadType method of extract mode.
        data_format (ExtractFormat): format of the extract.
        location (str): uri that identifies from where to extract data in the modelified format.
        schema (str): schema to be parsed to StructType.
    """

    def __init__(
        self,
        name: str,
        method: ExtractMethod,
        options: dict[str, str],
        schema: StructType | None = None,
    ) -> None:
        """
        Initialize ExtractModelPyspark with the modelified parameters.
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


class ExtractModelFileAbstract(ExtractModelAbstract, ABC):
    """Abstract class for file extraction models."""


class ExtractModelFilePyspark(ExtractModelFileAbstract, ExtractModelPyspark):
    """Model for file extraction using PySpark."""

    def __init__(
        self,
        name: str,
        method: ExtractMethod,
        data_format: ExtractFormat,
        location: str,
        options: dict[str, str],
        schema: StructType | None = None,
    ) -> None:
        super().__init__(
            name=name,
            method=method,
            options=options,
            schema=schema,
        )
        self.data_format = data_format
        self.location = location

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an ExtractModelFilePyspark object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            ExtractModelFilePyspark: The ExtractModelFilePyspark object created from the Confeti dictionary.
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


ExtractModelT = TypeVar("ExtractModelT", bound=ExtractModelAbstract)


class ExtractAbstract(Generic[ExtractModelT, DataFrameT], ABC):
    """Extract abstract class."""

    extract_model_concrete: type[ExtractModelT]

    def __init__(self, model: ExtractModelT) -> None:
        self.model = model
        self.data_registry = DataFramePysparkRegistry()

    @property
    def model(self) -> ExtractModelT:
        return self._model

    @model.setter
    def model(self, value: ExtractModelT) -> None:
        self._model = value

    @property
    def data_registry(self) -> DataFramePysparkRegistry:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFramePysparkRegistry) -> None:
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """Create an instance of ExtractAbstract from configuration."""
        model = cls.extract_model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _extract_batch(self) -> DataFrameT: ...

    @abstractmethod
    def _extract_streaming(self) -> DataFrameT: ...

    @abstractmethod
    def extract(self) -> None: ...


class ExtractContextAbstract(ABC):
    """Extract context abstract class for handling extraction strategies."""

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[ExtractAbstract]:
        """
        Get an extract class based on the extract format using the decorator registry.

        Args:
            confeti (dict[str, Any]): Configuration dictionary.

        Returns:
            Type[ExtractAbstract]: An extract implementation class.

        Raises:
            NotImplementedError: If the specified extract format is not supported.
        """
        try:
            extract_format = ExtractFormat(confeti[DATA_FORMAT])
            return ExtractRegistry.get(extract_format)
        except KeyError as e:
            raise NotImplementedError(f"Extract format {confeti.get(DATA_FORMAT, 'unknown')} is not supported.") from e


# Create a specific registry for Extract implementations
class ExtractRegistry(DecoratorRegistry[ExtractFormat, ExtractAbstract]):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """

    pass
