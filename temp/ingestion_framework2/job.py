"""
Job interface and implementations for various data processing jobs.

This module provides abstract classes and implementations for ETL job execution
across various engines and data formats.
"""

from abc import ABC
from enum import Enum
from typing import Any, Final, Generic, Self

from ingestion_framework.types import DataFrameT, DecoratorRegistrySingleton, StreamingQueryT
from ingestion_framework2.exceptions import DictKeyError
from ingestion_framework2.extract import ExtractAbstract, ExtractContextAbstract, ExtractModelAbstract
from ingestion_framework2.functions import FunctionAbstract
from ingestion_framework2.load import LoadAbstract, LoadContextAbstract, LoadModelAbstract
from ingestion_framework2.transform import TransformAbstract, TransformModelAbstract

ENGINE: Final[str] = "engine"
EXTRACTS: Final[str] = "extracts"
TRANSFORMS: Final[str] = "transforms"
LOADS: Final[str] = "loads"


class Engine(Enum):
    """Enumeration for job engines."""

    PYSPARK = "pyspark"


class JobAbstract(Generic[DataFrameT, StreamingQueryT], ABC):
    """
    Abstract base class to perform data extraction, transformations and loading (ETL).

    This class defines the core components of an ETL job and provides a standard
    interface for different engine implementations.
    """

    extract_concrete: type[ExtractContextAbstract]
    transform_concrete: type[TransformAbstract]
    load_concrete: type[LoadContextAbstract]

    def __init__(
        self,
        engine: Engine,
        extracts: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]],
        transforms: list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]],
        loads: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]],
    ) -> None:
        """
        Initialize Job instance.

        Args:
            engine: The engine type to use for processing.
            extracts: List of extract operations to perform.
            transforms: List of transform operations to perform.
            loads: List of load operations to perform.
        """
        self.engine = engine
        self.extracts = extracts
        self.transforms = transforms
        self.loads = loads

    @property
    def engine(self) -> Engine:
        return self._engine

    @engine.setter
    def engine(self, value: Engine) -> None:
        self._engine = value

    @property
    def extracts(self) -> list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]:
        return self._extracts

    @extracts.setter
    def extracts(self, value: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]) -> None:
        self._extracts = value

    @property
    def transforms(
        self,
    ) -> list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]]:
        return self._transforms

    @transforms.setter
    def transforms(
        self,
        value: list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]],
    ) -> None:
        self._transforms = value

    @property
    def loads(
        self,
    ) -> list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]:
        return self._loads

    @loads.setter
    def loads(self, value: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]) -> None:
        self._loads = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a job instance from configuration dictionary.

        Args:
            confeti: Configuration dictionary containing job specifications.
                Must contain 'engine' and 'extracts' keys, and optionally
                'transforms' and 'loads' keys.

        Returns:
            A new instance of the job class.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
            ValueError: If the engine in the configuration is not supported.
        """
        extracts: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]] = []
        transforms: list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]] = []
        loads: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]] = []

        try:
            engine = Engine(confeti[ENGINE])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        # Don't use registry here to avoid circular dependency
        # The subclass will handle its own instantiation
        for extract in confeti[EXTRACTS]:
            extract_cls = cls.extract_concrete.factory(extract)
            extract_instance = extract_cls.from_confeti(extract)
            extracts.append(extract_instance)

        for transform in confeti.get(TRANSFORMS, []):
            transform_instance = cls.transform_concrete.from_confeti(transform)
            transforms.append(transform_instance)

        for load in confeti.get(LOADS, []):
            load_class = cls.load_concrete.factory(load)
            load_instance = load_class.from_confeti(load)
            loads.append(load_instance)

        return cls(engine, extracts, transforms, loads)

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        for extract in self.extracts:
            extract.extract()

        for transform in self.transforms:
            transform.transform()

        for load in self.loads:
            load.load()


class JobRegistry(DecoratorRegistrySingleton[Engine, JobAbstract]):
    """
    Registry for Job implementations.

    Maps JobFormat enum values to concrete JobAbstract implementations.
    """


class JobContext:
    """
    Factory class to create JobModel instances.
    """

    @staticmethod
    def from_confeti(confeti: dict[str, Any]) -> type[JobAbstract]:
        """
        Create an appropriate job class based on the format specified in the configuration.

        This factory method uses the JobRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            confeti: Configuration dictionary that must include a 'data_format' key
                compatible with the JobFormat enum

        Returns:
            The concrete jobion class for the specified format

        Raises:
            NotImplementedError: If the specified job format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        try:
            engine = Engine(confeti[ENGINE])
            return JobRegistry.get(engine)
        except KeyError as e:
            format_name = confeti.get(ENGINE, "<missing>")
            raise NotImplementedError(f"Job format {format_name} is not supported.") from e
