"""
Load interface and implementations for various data formats.

This module provides abstract classes and implementations for data loading
to various destinations and formats.
"""

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql.streaming.query import StreamingQuery

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton, StreamingQueryRegistry
from ingestion_framework.utils.spark import SparkHandler

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
METHOD: Final[str] = "method"
MODE: Final[str] = "mode"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA_LOCATION: Final[str] = "schema_location"
OPTIONS: Final[str] = "options"

LoadModelT = TypeVar("LoadModelT", bound="LoadModel")


class LoadRegistry(RegistryDecorator, metaclass=Singleton):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadMode(Enum):
    """Enumeration for methods of load modes."""

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


class LoadModel(ABC):
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
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            location (str): URI that identifies where to load data in the modelified format.
            schema_location (str): URI that identifies where to load schema.
            options (dict[str, Any]): Options for the sink input.
        """
        self.name = name
        self.upstream_name = upstream_name
        self.method = method
        self.location = location
        self.schema_location = schema_location
        self.options = options

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

    @property
    def schema_location(self) -> str | None:
        return self._schema_location

    @schema_location.setter
    def schema_location(self, value: str | None) -> None:
        self._schema_location = value

    @property
    def options(self) -> dict[str, str]:
        return self._options

    @options.setter
    def options(self, value: dict[str, str]) -> None:
        self._options = value

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


class Load(Generic[LoadModelT], ABC):
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
        self.data_registry = DataFrameRegistry()
        self.streaming_query_registry = StreamingQueryRegistry()

    @property
    def model(self) -> LoadModelT:
        """Get the loading model configuration."""
        return self._model

    @model.setter
    def model(self, value: LoadModelT) -> None:
        """Set the loading model configuration."""
        self._model = value

    @property
    def data_registry(self) -> DataFrameRegistry:
        """Get the data registry containing data to be loaded."""
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFrameRegistry) -> None:
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
        model = cls.load_model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Perform batch loading of data to the destination.
        """

    @abstractmethod
    def _load_streaming(self) -> StreamingQuery:
        """
        Perform streaming loading of data to the destination.

        Returns:
            A streaming query object that can be used to monitor the stream
        """

    def _load_schema(self) -> None:
        """
        load schema from DataFrame.
        """
        if self.model.schema_location is None:
            return

        schema = json.dumps(self.data_registry[self.model.name].schema.jsonValue())

        with open(self.model.schema_location, mode="w", encoding="utf-8") as f:
            f.write(schema)

    def load(self) -> None:
        """
        load data with PySpark.
        """
        spark_handler: SparkHandler = SparkHandler()
        spark_handler.add_configs(options=self.model.options)

        # Copy the dataframe from upstream to current name
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        if self.model.method == LoadMethod.BATCH:
            self._load_batch()
        elif self.model.method == LoadMethod.STREAMING:
            self.streaming_query_registry[self.model.name] = self._load_streaming()
        else:
            raise ValueError(f"Loading method {self.model.method} is not supported for Pyspark.")

        self._load_schema()


class LoadModelFile(LoadModel):
    """Abstract base class for file-based load models."""

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        mode: LoadMode,
        data_format: LoadFormat,
        location: str,
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        super().__init__(
            name=name,
            upstream_name=upstream_name,
            method=method,
            location=location,
            schema_location=schema_location,
            options=options,
        )
        self.mode = mode
        self.data_format = data_format

    @property
    def mode(self) -> LoadMode:
        return self._mode

    @mode.setter
    def mode(self, value: LoadMode) -> None:
        self._mode = value

    @property
    def data_format(self) -> LoadFormat:
        return self._data_format

    @data_format.setter
    def data_format(self, value: LoadFormat) -> None:
        self._data_format = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a LoadModelFilePyspark object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            LoadModelFilePyspark: LoadModelFilePyspark object.
        """
        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
            method = LoadMethod(confeti[METHOD])
            mode = LoadMode(confeti[MODE])
            data_format = LoadFormat(confeti[DATA_FORMAT])
            location = confeti[LOCATION]
            schema_location = confeti.get(SCHEMA_LOCATION, None)
            options = confeti.get(OPTIONS, {})
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(
            name=name,
            upstream_name=upstream_name,
            method=method,
            mode=mode,
            data_format=data_format,
            location=location,
            schema_location=schema_location,
            options=options,
        )


@LoadRegistry.register(LoadFormat.PARQUET)
@LoadRegistry.register(LoadFormat.JSON)
@LoadRegistry.register(LoadFormat.CSV)
class LoadFile(Load[LoadModelFile]):
    """
    Concrete class for file loadion using PySpark DataFrame.
    """

    load_model_concrete = LoadModelFile

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

    def _load_streaming(self) -> StreamingQuery:
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


class LoadContext:
    """
    Abstract context class for creating and managing loading strategies.

    This class implements the Strategy pattern for data loading, allowing
    different loading implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, confeti: dict[str, type[Load]]) -> type[Load[LoadModel]]:
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
        data_format = confeti[DATA_FORMAT]

        try:
            load_format = LoadFormat(data_format)
            return LoadRegistry.get(load_format)
        except KeyError as e:
            raise NotImplementedError(f"Load format {data_format} is not supported.") from e
