"""
Job interface and implementations for various data processing jobs.

This module provides abstract classes and implementations for ETL job execution
across various engines and data formats.
"""

from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Any, Final, Generic, Self

from pyspark.sql.streaming.query import StreamingQuery

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.extract import (
    ExtractAbstract,
    ExtractContextAbstract,
    ExtractModelAbstract,
)
from ingestion_framework.functions import FunctionAbstract
from ingestion_framework.load import (
    LoadAbstract,
    LoadContextAbstract,
    LoadModelAbstract,
)
from ingestion_framework.pyspark.extract import ExtractContextPyspark
from ingestion_framework.pyspark.load import LoadContextPyspark
from ingestion_framework.pyspark.transform import TransformPyspark
from ingestion_framework.transform import TransformAbstract, TransformModelAbstract
from ingestion_framework.types import DataFrameT, DecoratorRegistry, StreamingQueryT
from ingestion_framework.utils.file_handler import FileHandlerContext

ENGINE: Final[str] = "engine"
EXTRACTS: Final[str] = "extracts"
TRANSFORMS: Final[str] = "transforms"
LOADS: Final[str] = "loads"


class Engine(Enum):
    """Enumeration for job engines."""

    PYSPARK = "pyspark"


class JobAbstract(Generic[DataFrameT, StreamingQueryT], ABC):
    """
    Job class to perform data extraction, transformations and loading (ETL).
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
            engine (OptionsEngine): Engine type.
            extracts (list[ExtractAbstract]): Extract modelifications.
            transforms (list[TransformAbstract]): Transform modelifications.
            loads (list[LoadAbstract]): Load modelifications.
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
        Get the job modelifications from confeti.

        Args:
            confeti (dict[str, Any]): confeti.

        Returns:
            Job: model.

        Raises:
            ValueError: If the engine in the confeti is not supported.
        """
        extracts = []
        transforms = []
        loads = []

        try:
            engine = Engine(confeti[ENGINE])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        # Don't use registry here to avoid circular dependency
        # The subclass will handle its own instantiation
        for extract in confeti[EXTRACTS]:
            extract_class = cls.extract_concrete.factory(extract)
            extract_instance = extract_class.from_confeti(extract)
            extracts.append(extract_instance)

        for transform in confeti.get(TRANSFORMS, []):
            transform_instance = cls.transform_concrete.from_confeti(transform)
            transforms.append(transform_instance)

        for load in confeti.get(LOADS, []):
            load_class = cls.load_concrete.factory(load)
            load_instance = load_class.from_confeti(load)
            loads.append(load_instance)

        return cls(engine, extracts, transforms, loads)

    def run(self) -> list[StreamingQuery]:
        """
        Run the job.

        Returns:
            list[StreamingQuery]: stream execution query.
        """
        queries = []

        for extract in self.extracts:
            extract.extract()

        for transform in self.transforms:
            transform.transform()

        for load in self.loads:
            load.load()
            if load.model.method.value == "streaming":
                queries.append(load.data_registry[load.model.name])

        return queries

    @classmethod
    def from_confeti_path(cls, path: Path) -> Self:
        """
        Create a job instance from a Confeti file.

        Args:
            path (Path): Path to the Confeti file.

        Returns:
            Job: Job instance.
        """
        file_handler = FileHandlerContext.factory(filepath=str(path))
        confeti = file_handler.read()
        return cls.from_confeti(confeti)


# Create a registry for Job implementations
class JobRegistry(DecoratorRegistry[Engine, JobAbstract]):
    """
    Registry for Job implementations.

    Maps Engine enum values to concrete JobAbstract implementations.
    """


# For backward compatibility with existing code
class Job(JobAbstract):
    """
    Legacy Job class for backward compatibility.

    Use JobPyspark instead for new code.
    """

    extract_concrete = ExtractContextPyspark
    transform_concrete = TransformPyspark
    load_concrete = LoadContextPyspark

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

    @classmethod
    def from_file(cls, filepath: str) -> "Job":
        """
        Legacy method for backward compatibility.

        Args:
            filepath (str): Path to the confeti file

        Returns:
            Job instance
        """
        return cls.from_confeti_path(Path(filepath))
