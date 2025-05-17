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
    DecoratorRegistrySingleton,
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
    """
    Abstract base class for load operation models.

    This class defines the configuration model for data loading operations,
    specifying the name, upstream source, method, and destination for the load.
    """

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        location: str,
    ) -> None:
        """
        Initialize the loading model with basic parameters.

        Args:
            name: Identifier for this loading operation
            upstream_name: Identifier for the source data to load
            method: Method of loading to use (batch or streaming)
            location: URI where data will be loaded to
        """
        self.name = name
        self.upstream_name = upstream_name
        self.method = method
        self.location = location

    @property
    def name(self) -> str:
        """Get the name of the loading operation."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set the name of the loading operation."""
        self._name = value

    @property
    def upstream_name(self) -> str:
        """Get the name of the upstream data source."""
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        """Set the name of the upstream data source."""
        self._upstream_name = value

    @property
    def method(self) -> LoadMethod:
        """Get the loading method (batch or streaming)."""
        return self._method

    @method.setter
    def method(self, value: LoadMethod) -> None:
        """Set the loading method (batch or streaming)."""
        self._method = value

    @property
    def location(self) -> str:
        """Get the destination URI for the loaded data."""
        return self._location

    @location.setter
    def location(self, value: str) -> None:
        """Set the destination URI for the loaded data."""
        self._location = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a loading model from a configuration dictionary.

        Args:
            confeti: Configuration dictionary containing loading parameters

        Returns:
            An initialized loading model

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        ...


class LoadModelFileAbstract(LoadModelAbstract):
    """Abstract base class for file-based load models."""


LoadModelT = TypeVar("LoadModelT", bound=LoadModelAbstract)


class LoadAbstract(Generic[LoadModelT, DataFrameT, StreamingQueryT], ABC):
    """
    Abstract base class for data loading operations.

    This class defines the interface for all loading implementations,
    supporting both batch and streaming loads to various destinations.
    """

    load_model_concrete: type[LoadModelT]

    def __init__(self, model: LoadModelT) -> None:
        """
        Initialize the loading operation.

        Args:
            model: Configuration model for the loading operation
        """
        self.model = model
        self.data_registry = DataFramePysparkRegistry()

    @property
    def model(self) -> LoadModelT:
        """Get the loading model configuration."""
        return self._model

    @model.setter
    def model(self, value: LoadModelT) -> None:
        """Set the loading model configuration."""
        self._model = value

    @property
    def data_registry(self) -> DataFramePysparkRegistry:
        """Get the data registry containing data to be loaded."""
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFramePysparkRegistry) -> None:
        """Set the data registry containing data to be loaded."""
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a loading instance from a configuration dictionary.

        Args:
            confeti: Configuration dictionary containing loading specifications

        Returns:
            An initialized loading instance

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        model: LoadModelT = cls.load_model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Perform batch loading of data to the destination.
        """

    @abstractmethod
    def _load_streaming(self) -> StreamingQueryT:
        """
        Perform streaming loading of data to the destination.

        Returns:
            A streaming query object that can be used to monitor the stream
        """

    @abstractmethod
    def _load_schema(self) -> None:
        """
        Load the schema information to the destination if needed.
        """

    @abstractmethod
    def load(self) -> None:
        """
        Execute the loading operation based on the configured method.

        This method should determine whether to use batch or streaming loading
        based on the model configuration, and handle any schema requirements.
        """


class LoadFileAbstract(
    LoadAbstract[LoadModelT, DataFrameT, StreamingQueryT],
    Generic[LoadModelT, DataFrameT, StreamingQueryT],
    ABC,
):
    """
    Abstract class for file loadion.
    """


class LoadContextAbstract(ABC):
    """
    Abstract context class for creating and managing loading strategies.

    This class implements the Strategy pattern for data loading, allowing
    different loading implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[LoadAbstract]:
        """
        Create an appropriate load class based on the format specified in the configuration.

        This factory method uses the LoadRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            confeti: Configuration dictionary that must include a 'data_format' key
                compatible with the LoadFormat enum

        Returns:
            The concrete loading class for the specified format

        Raises:
            NotImplementedError: If the specified load format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        try:
            load_format = LoadFormat(confeti[DATA_FORMAT])
            return LoadRegistry.get(load_format)
        except KeyError as e:
            format_name = confeti.get(DATA_FORMAT, "<missing>")
            raise NotImplementedError(f"Load format {format_name} is not supported.") from e


# Create a specific registry for Load implementations - define after LoadAbstract to avoid circular imports
class LoadRegistry(DecoratorRegistrySingleton[LoadFormat, LoadAbstract]):
    """
    Registry for Load implementations.

    Maps LoadFormat enum values to concrete LoadAbstract implementations.
    """
