"""
Base classes for data transformation operations.

This module provides abstract and concrete base classes for defining data
transformation operations in the ingestion framework. It implements interfaces and models
for transforming data between extraction and loading stages.
"""

from abc import ABC
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.functions.base import FunctionAbstract, FunctionPyspark
from ingestion_framework.transforms.functions.select import SelectFunctionPyspark
from ingestion_framework.types import DataFrameT, RegistrySingleton

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"


NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


class TransformModelAbstract(ABC):
    """
    Abstract base class for transformation models.

    This class represents the configuration of a transformation, including its
    name, upstream dependency, and any parameters needed for the transformation.

    Attributes:
        name (str): The ID of the transformation.
        upstream_name (str): The ID of the upstream data dependency.
    """

    def __init__(self, name: str, upstream_name: str) -> None:
        """
        Initialize a transformation model.

        Args:
            name (str): The ID of the transformation.
            upstream_name (str): The ID of the upstream data dependency.
        """
        self.name = name
        self.upstream_name = upstream_name

    @property
    def name(self) -> str:
        """
        Get the transformation name.

        Returns:
            str: The transformation name.
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the transformation name.

        Args:
            value (str): The transformation name to set.
        """
        self._name = value

    @property
    def upstream_name(self) -> str:
        """
        Get the upstream dependency name.

        Returns:
            str: The upstream dependency name.
        """
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        """
        Set the upstream dependency name.

        Args:
            value (str): The upstream dependency name to set.
        """
        self._upstream_name = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a TransformModelAbstract object from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            TransformModelAbstract: The transformation model created from the configuration.

        Raises:
            DictKeyError: If a required key is missing from the configuration.

        Examples:
            >>> confeti = {
            >>>     "name": "bronze-test-transform-dev",
            >>>     "upstream_name": "bronze-test-extract-dev",
            >>>     "functions": [
            >>>         {"function": "cast", "arguments": {"columns": {"age": "LongType"}}},
            >>>     ],
            >>> }
            >>> model = TransformModelAbstract.from_confeti(confeti)
        """
        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(name=name, upstream_name=upstream_name)


class TransformModelPyspark(TransformModelAbstract):
    """
    PySpark-specific implementation of transformation model.

    This class extends TransformModelAbstract to provide PySpark-specific
    functionality for data transformations.

    Examples:
        >>> df = spark.createDataFrame(data=[("Alice", 27), ("Bob", 32),], schema=["name", "age"])
        >>> dict = {"function": "cast", "arguments": {"columns": {"age": "StringType",}}}
        >>> transform = TransformFunction.from_dict(dict=dict)
        >>> df = df.transform(func=transform).printSchema()
        root
        |-- name: string (nullable = true)
        |-- age: string (nullable = true)
    """


TransformModelT = TypeVar("TransformModelT", bound=TransformModelAbstract)
FunctionT = TypeVar("FunctionT", bound=FunctionAbstract)


class TransformAbstract(Generic[TransformModelT, FunctionT, DataFrameT], ABC):
    """
    Abstract base class for transformations.

    This class represents a transformation operation that can be applied to data.
    It consists of a model that defines the transformation and one or more functions
    that implement the actual transformation logic.

    Attributes:
        model (TransformModelT): The transformation model.
        functions (list[FunctionT]): List of transformation functions.
        data_registry (RegistrySingleton): Registry for storing and retrieving DataFrames.
    """

    load_model_concrete: type[TransformModelT]
    SUPPORTED_FUNCTIONS: dict[str, Any]

    def __init__(self, model: TransformModelT, functions: list[FunctionT]) -> None:
        """
        Initialize a transformation with a model and functions.

        Args:
            model (TransformModelT): The transformation model.
            functions (list[FunctionT]): List of transformation functions.
        """
        self.model = model
        self.functions = functions
        self.data_registry = RegistrySingleton()

    @property
    def model(self) -> TransformModelT:
        """
        Get the transformation model.

        Returns:
            TransformModelT: The transformation model.
        """
        return self._model

    @model.setter
    def model(self, value: TransformModelT) -> None:
        """
        Set the transformation model.

        Args:
            value (TransformModelT): The transformation model to set.
        """
        self._model = value

    @property
    def functions(self) -> list[FunctionT]:
        """
        Get the list of transformation functions.

        Returns:
            list[FunctionT]: The list of transformation functions.
        """
        return self._functions

    @functions.setter
    def functions(self, value: list[FunctionT]) -> None:
        """
        Set the list of transformation functions.

        Args:
            value (list[FunctionT]): The list of transformation functions to set.
        """
        self._functions = value

    @property
    def data_registry(self) -> RegistrySingleton:
        """
        Get the data registry.

        Returns:
            RegistrySingleton: The data registry.
        """
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: RegistrySingleton) -> None:
        """
        Set the data registry.

        Args:
            value (RegistrySingleton): The data registry to set.
        """
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation from a configuration dictionary.

        This method processes the configuration to create a transformation model
        and a list of transformation functions.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            Self: A new transformation instance.

        Raises:
            NotImplementedError: If a function specified in the configuration is not supported.
            DictKeyError: If a required key is missing from the configuration.

        Examples:
            >>> confeti = {
            >>>     "name": "transform-example",
            >>>     "upstream_name": "extract-example",
            >>>     "functions": [
            >>>         {"function": "select", "arguments": {"columns": ["name", "age"]}},
            >>>     ]
            >>> }
            >>> transform = TransformAbstract.from_confeti(confeti)
        """
        model: TransformModelT = cls.load_model_concrete.from_confeti(confeti=confeti)

        functions = []

        for function_confeti in confeti.get(FUNCTIONS, []):
            function_name: str = function_confeti[FUNCTION]

            if function_name not in cls.SUPPORTED_FUNCTIONS.keys():
                raise NotImplementedError(f"{FUNCTION} {function_name} is not supported.")

            function_concrete: FunctionT = cls.SUPPORTED_FUNCTIONS[function_name]
            function_ = function_concrete.from_confeti(confeti=function_confeti)
            functions.append(function_)

        return cls(model=model, functions=functions)

    def transform(self) -> None:
        """
        Apply all transformation functions to the data.

        This method applies each transformation function in sequence to the data
        identified by the model's name in the data registry.
        """
        for function in self.functions:
            function.callable_(dataframe_registry=self.data_registry, dataframe_name=self.model.name)


class TransformPyspark(TransformAbstract[TransformModelPyspark, FunctionPyspark, DataFramePyspark]):
    """
    PySpark-specific implementation of transformation.

    This class extends TransformAbstract to provide PySpark-specific
    functionality for data transformations.
    """

    load_model_concrete = TransformModelPyspark
    SUPPORTED_FUNCTIONS: dict[str, Any] = {
        "select": SelectFunctionPyspark,
        # Additional transform functions can be added here
    }
