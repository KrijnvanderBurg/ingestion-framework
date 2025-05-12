"""
Job orchestration and execution for the ingestion framework.

This module provides classes and functions for defining, configuring, and executing
data ingestion jobs that coordinate extraction, transformation, and loading operations
within the ingestion framework.
"""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Final, Generic, Self

from pyspark.sql.streaming.query import StreamingQuery

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.extract.base import ExtractAbstract, ExtractModelAbstract
from ingestion_framework.extract.factory import ExtractContextAbstract, ExtractContextPyspark
from ingestion_framework.load.base import LoadAbstract, LoadModelAbstract
from ingestion_framework.load.factory import LoadContextAbstract, LoadContextPyspark
from ingestion_framework.transforms.base import (
    TransformAbstract,
    TransformModelAbstract,
    TransformPyspark,
)
from ingestion_framework.transforms.recipes.base import RecipeAbstract
from ingestion_framework.types import DataFrameT, StreamingQueryT
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
    Abstract base class for ETL job execution.

    This class defines the structure and flow for Extract, Transform, Load (ETL) jobs,
    providing a framework for concrete ETL implementations. It handles configurations
    for various data processing stages and orchestrates their execution.

    The job follows a generic approach that can work with different dataframe types
    and streaming query implementations through type parameters.

    Attributes:
        extract_concrete (type[ExtractContextAbstract]): The concrete class type for extraction context.
        transform_concrete (type[TransformAbstract]): The concrete class type for transformations.
        load_concrete (type[LoadContextAbstract]): The concrete class type for loading context.

    Generic Parameters:
        DataFrameT: The type of DataFrame used in the ETL process (e.g., Pandas DataFrame, Spark DataFrame).
        StreamingQueryT: The type of streaming query returned by streaming operations.

    Usage:
        Concrete implementations must inherit from this class and implement the abstract
        methods. The class is designed to be initialized with specific extract, transform,
        and load configurations, and executed as a single ETL job.

        Job configuration can be provided through a dictionary structure via the from_confeti
        class method, which handles instantiation of all extract, transform, and load components.
    """

    extract_concrete: type[ExtractContextAbstract]
    transform_concrete: type[TransformAbstract]
    load_concrete: type[LoadContextAbstract]

    def __init__(
        self,
        engine: Engine,
        extracts: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]],
        transforms: list[TransformAbstract[TransformModelAbstract, RecipeAbstract, DataFrameT]],
        loads: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]],
    ) -> None:
        """
        Initialize a job in the ingestion framework.

        This constructor sets up a data processing job with the specified components for data extraction,
        transformation, and loading.

        Parameters
        ----------
        engine : Engine
            The execution engine that will run the job operations.

        extracts : list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]
            A list of extract operations to retrieve data from source systems.
            Each extract should implement the ExtractAbstract interface.

        transforms : list[TransformAbstract[TransformModelAbstract, RecipeAbstract, DataFrameT]]
            A list of transform operations to manipulate the extracted data.
            Each transform should implement the TransformAbstract interface.

        loads : list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]
            A list of load operations to store the transformed data in target systems.
            Each load should implement the LoadAbstract interface.

        Returns
        -------
        None
        """
        self.engine = engine
        self.extracts = extracts
        self.transforms = transforms
        self.loads = loads

    @property
    def engine(self) -> Engine:
        """
        Get the engine type for the job.

        Returns:
            Engine: The engine type.
        """
        return self._engine

    @engine.setter
    def engine(self, value: Engine) -> None:
        self._engine = value

    @property
    def extracts(self) -> list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]:
        """
        Get the list of extract configurations.

        Returns:
            list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]: The list of extract configurations.
        """
        return self._extracts

    @extracts.setter
    def extracts(self, value: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]) -> None:
        self._extracts = value

    @property
    def transforms(
        self,
    ) -> list[TransformAbstract[TransformModelAbstract, RecipeAbstract, DataFrameT]]:
        """
        Get the list of transform configurations.

        Returns:
            list[TransformAbstract[TransformModelAbstract, RecipeAbstract, DataFrameT]]:
                The list of transform configurations.
        """
        return self._transforms

    @transforms.setter
    def transforms(self, value: list[TransformAbstract[TransformModelAbstract, RecipeAbstract, DataFrameT]]) -> None:
        self._transforms = value

    @property
    def loads(self) -> list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]:
        """
        Returns the list of load objects associated with this job.

        Returns:
            list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]: A list of load objects that implement
            the LoadAbstract interface with specific type parameters for model, dataframe, and streaming query.
        """
        return self._loads

    @loads.setter
    def loads(self, value: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]) -> None:
        self._loads = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Get the job modelifications from confeti.

        Args:
            confeti (dict[str, Any]): dictionary object.

        Returns:
            Job: job instance.
        """
        try:
            engine = Engine(value=confeti[ENGINE])

            extracts: list[ExtractAbstract] = []
            for extract_confeti in confeti[EXTRACTS]:
                extract_strategy = cls.extract_concrete.factory(confeti=extract_confeti)
                extract = extract_strategy.from_confeti(confeti=extract_confeti)
                extracts.append(extract)

            transforms: list[TransformAbstract] = []
            for transform_confeti in confeti[TRANSFORMS]:
                transform = cls.transform_concrete.from_confeti(confeti=transform_confeti)
                transforms.append(transform)

            loads: list[LoadAbstract] = []
            for load_confeti in confeti[LOADS]:
                load_strategy = cls.load_concrete.factory(confeti=load_confeti)
                load = load_strategy.from_confeti(confeti=load_confeti)
                loads.append(load)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(engine=engine, transforms=transforms, extracts=extracts, loads=loads)

    @abstractmethod
    def execute(self) -> StreamingQuery | None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        raise NotImplementedError

    def _extract(self) -> None:
        """
        Extract data from modelification into a DataFrame.
        """
        for extract in self.extracts:
            extract.extract()

    def _transform(self) -> None:
        """
        Transform data from modelifiction.

        Args:
            df (DataFrame): Dataframe to be transformed.

        Returns:
            DataFrame: transformed data.
        """
        for transform in self.transforms:
            transform.data_registry[transform.model.name] = transform.data_registry[transform.model.upstream_name]
            transform.transform()

    def _load(self) -> None:
        """
        Load data to the modelification.

        Returns:
            DataFrame: The loaded data.
        """
        for load in self.loads:
            load.data_registry[load.model.name] = load.data_registry[load.model.upstream_name]
            load.load()


class JobPyspark(JobAbstract):
    """
    PySpark implementation of the ETL job.

    This class provides a concrete implementation of the JobAbstract class for
    PySpark-based data processing. It sets up appropriate context classes for
    extraction, transformation, and loading operations in a PySpark environment.

    Attributes:
        extract_concrete: The PySpark-specific extraction context class.
        transform_concrete: The PySpark-specific transformation class.
        load_concrete: The PySpark-specific loading context class.

    Examples:
        >>> job_config = {
        >>>     "engine": "pyspark",
        >>>     "extracts": [...],
        >>>     "transforms": [...],
        >>>     "loads": [...]
        >>> }
        >>> job = JobPyspark.from_confeti(job_config)
        >>> job.execute()
    """

    extract_concrete = ExtractContextPyspark
    transform_concrete = TransformPyspark
    load_concrete = LoadContextPyspark

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        self._extract()
        self._transform()
        self._load()


class Job:
    """
    Factory class to create JobModel instances.
    """

    @classmethod
    def from_file(cls, filepath: str) -> JobPyspark:
        """
        Get the job modelifications from confeti.

        Args:
            filepath (str): path to file.

        Returns:
            Job: job instance.
        """
        handler = FileHandlerContext.factory(filepath=filepath)
        file: dict[str, Any] = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_confeti(confeti=file)

        raise NotImplementedError("No handling options found.")

    @staticmethod
    def from_confeti(confeti: dict[str, Any]) -> JobPyspark:
        """
        Create a JobModel instance from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            JobModelAbstract: The JobModel instance created from the Confeti dictionary.

        Raises:
            DictKeyError: If a required key is missing in the Confeti dictionary.
            NotImplementedError: If the options value is not recognized or not supported.
        """
        try:
            engine = Engine(value=confeti[ENGINE])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        if engine == Engine.PYSPARK:
            return JobPyspark.from_confeti(confeti=confeti)

        raise NotImplementedError(f"No engine handling recognised or supported for value: {engine}")
