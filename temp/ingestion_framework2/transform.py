"""
Transform interface and implementations for various data transformations.

This module provides abstract classes and implementations for data transformations
using various function registrations.
"""

from abc import ABC
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.types import (
    DataFramePysparkRegistry,
    DataFrameT,
    DecoratorRegistrySingleton,
)
from ingestion_framework2.exceptions import DictKeyError
from ingestion_framework2.functions import FunctionAbstract

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


class TransformModelAbstract(ABC):
    """
    Modelification for data transformation.

    Args:
        name (str): The ID of the transformation modelification.
        functions (list): List of transformation functions.
    """

    def __init__(self, name: str, upstream_name: str) -> None:
        self.name = name
        self.upstream_name = upstream_name

    @property
    def name(self) -> str:
        """
        Get the name of the transformation.

        Returns:
            str: The transformation name.
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the name of the transformation.

        Args:
            value (str): The transformation name to set.
        """
        self._name = value

    @property
    def upstream_name(self) -> str:
        """Get the name of the upstream data source.

        Returns:
            str: The upstream data source name.
        """
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        """Set the upstream data source name.

        Args:
            value (str): The upstream data source name to set.
        """
        self._upstream_name = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a TransformModelAbstract object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            TransformModelAbstract: The TransformModelAbstract object created from the Confeti dictionary.

        Example:
            >>> "transforms": [
            >>>     {
            >>>         "name": "bronze-test-transform-dev",
            >>>         "upstream_name": ["bronze-test-extract-dev"],
            >>>         "functions": [
            >>>             {"function": "cast", "arguments": {"columns": {"age": "LongType"}}},
            >>>             // etc.
            >>>         ],
            >>>     }
            >>> ],
        """
        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(name=name, upstream_name=upstream_name)


TransformModelT = TypeVar("TransformModelT", bound=TransformModelAbstract)
FunctionT = TypeVar("FunctionT", bound=FunctionAbstract)


class TransformAbstract(Generic[TransformModelT, FunctionT, DataFrameT], ABC):
    """Transform abstract class."""

    load_model_concrete: type[TransformModelT]
    SUPPORTED_FUNCTIONS: dict[str, Any]

    def __init__(self, model: TransformModelT, functions: list[FunctionT]) -> None:
        self.model = model
        self.functions = functions
        self.data_registry = DataFramePysparkRegistry()

    @property
    def model(self) -> TransformModelT:
        return self._model

    @model.setter
    def model(self, value: TransformModelT) -> None:
        self._model = value

    @property
    def functions(self) -> list[FunctionT]:
        return self._functions

    @functions.setter
    def functions(self, value: list[FunctionT]) -> None:
        self._functions = value

    @property
    def data_registry(self) -> DataFramePysparkRegistry:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFramePysparkRegistry) -> None:
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an instance of TransformAbstract from configuration.

        Args:
            confeti: Configuration dictionary containing transformation specifications.
                Must contain 'name' and 'upstream_name' keys, and optionally a 'functions' list.

        Returns:
            A new instance of the transformation class.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
            NotImplementedError: If a specified function is not supported.
        """
        model: TransformModelT = cls.load_model_concrete.from_confeti(confeti=confeti)
        functions: list[FunctionT] = []

        for function_confeti in confeti.get(FUNCTIONS, []):
            function_name: str = function_confeti[FUNCTION]

            if function_name not in cls.SUPPORTED_FUNCTIONS:
                raise NotImplementedError(f"{FUNCTION} {function_name} is not supported.")

            function_concrete: type[FunctionT] = cls.SUPPORTED_FUNCTIONS[function_name]
            function_instance = function_concrete.from_confeti(confeti=function_confeti)
            functions.append(function_instance)

        return cls(model=model, functions=functions)

    def transform(self) -> None:
        """
        Apply all transformation functions to the data source.

        This method performs the following steps:
        1. Copies the dataframe from the upstream source to current transform's name
        2. Sequentially applies each transformation function to the dataframe
        3. Each function updates the registry with its results

        Note:
            Functions are applied in the order they were defined in the configuration.
        """
        # Copy the dataframe from upstream to current name
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        # Apply transformations sequentially
        for function in self.functions:
            function.callable_(dataframe_registry=self.data_registry, dataframe_name=self.model.name)


# Create a registry for Transform implementations
class TransformRegistry(DecoratorRegistrySingleton[str, TransformAbstract]):
    """
    Registry for Transform implementations.

    Maps function names to concrete TransformAbstract implementations.
    """
