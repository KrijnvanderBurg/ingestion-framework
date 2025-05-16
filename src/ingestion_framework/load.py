"""
Load interface and implementations for various data formats.

This module provides abstract classes and implementations for data loading
to various destinations and formats.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.types import (
    DataFramePysparkRegistry,
    DataFrameT,
    DecoratorRegistry,
    StreamingQueryT,
)

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
METHOD: Final[str] = "method"
MODE: Final[str] = "mode"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA_LOCATION: Final[str] = "schema_location"
OPTIONS: Final[str] = "options"


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadMode(Enum):
    """Enumeration for methods of load modes.

    pyspark.sql.DataFrameWriter.mode:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html
        * ``append``: Append contents of this :class:`DataFrame` to existing data.
        * ``overwrite``: Overwrite existing data.
        * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
            exists.
        * ``ignore``: Silently ignore this operation if data already exists.

        Options include:
        * ``append``: Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * ``complete``: All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these are some updates
        * ``update``: only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.
    """

    # pyspark batch
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"

    # pyspark streaming
    # APPEND = "append"  # already added above for pyspark batch
    COMPLETE = "complete"
    UPDATE = "update"


class LoadFormat(Enum):
    """Enumeration for methods of input and structures for the load."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class LoadModelAbstract(ABC):
    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        location: str,
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            location (str): URI that identifies where to load data in the modelified format.
        """
        self.name = name
        self.upstream_name = upstream_name
        self.method = method
        self.location = location

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def upstream_name(self) -> str:
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        self._upstream_name = value

    @property
    def method(self) -> LoadMethod:
        return self._method

    @method.setter
    def method(self, value: LoadMethod) -> None:
        self._method = value

    @property
    def location(self) -> str:
        return self._location

    @location.setter
    def location(self, value: str) -> None:
        self._location = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class LoadModelFileAbstract(LoadModelAbstract):
    """Abstract base class for file-based load models."""


LoadModelT = TypeVar("LoadModelT", bound=LoadModelAbstract)


class LoadAbstract(Generic[LoadModelT, DataFrameT, StreamingQueryT], ABC):
    """Load abstract class."""

    load_model_concrete: type[LoadModelT]

    def __init__(self, model: LoadModelT) -> None:
        self.model = model
        self.data_registry = DataFramePysparkRegistry()

    @property
    def model(self) -> LoadModelT:
        return self._model

    @model.setter
    def model(self, value: LoadModelT) -> None:
        self._model = value

    @property
    def data_registry(self) -> DataFramePysparkRegistry:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFramePysparkRegistry) -> None:
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """Create an instance of LoadAbstract from configuration."""
        model: LoadModelT = cls.load_model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Abstract method for batch loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def _load_streaming(self) -> StreamingQueryT:
        """
        Abstract method for streaming loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def _load_schema(self) -> None:
        """
        Abstract method for schema loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self) -> None:
        """
        Main loadion method.
        """
        raise NotImplementedError


class LoadFileAbstract(
    LoadAbstract[LoadModelT, DataFrameT, StreamingQueryT],
    Generic[LoadModelT, DataFrameT, StreamingQueryT],
    ABC,
):
    """
    Abstract class for file loadion.
    """


class LoadContextAbstract(ABC):
    """Abstract class representing a strategy context for creating data loads."""

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[LoadAbstract]:
        """
        Get a load class based on the load format using the decorator registry.

        Args:
            confeti (dict[str, Any]): Configuration dictionary.

        Returns:
            Type[LoadAbstract]: A load implementation class.

        Raises:
            NotImplementedError: If the specified load format is not supported.
        """
        try:
            load_format = LoadFormat(confeti[DATA_FORMAT])
            return LoadRegistry.get(load_format)
        except KeyError as e:
            raise NotImplementedError(f"Load format {confeti[DATA_FORMAT]} is not supported.") from e


# Create a specific registry for Load implementations - define after LoadAbstract to avoid circular imports
class LoadRegistry(DecoratorRegistry[LoadFormat, LoadAbstract]):
    """
    Registry for Load implementations.

    Maps LoadFormat enum values to concrete LoadAbstract implementations.
    """
