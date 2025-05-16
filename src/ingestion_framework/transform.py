"""
Transform interface and implementations for various data transformations.

This module provides abstract classes and implementations for data transformations
using various function registrations.
"""

from abc import ABC
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.functions import FunctionAbstract
from ingestion_framework.types import (
    DataFramePysparkRegistry,
    DataFrameT,
    DecoratorRegistrySingleton,
)

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
        Returns:
            str
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Args:
            value (str)
        """
        self._name = value

    @property
    def upstream_name(self) -> str:
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
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
        """Create an instance of TransformAbstract from configuration."""
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
        Apply all transform functions on df.

        This method applies all transformation functions and stores the result in the registry
        under the transform's name, reading from the upstream_name.
        """
        # Copy the dataframe from upstream to current name
        self.data_registry[self.model.name] = self.data_registry[self.model.upstream_name]

        # Apply transformations
        for function in self.functions:
            function.callable_(dataframe_registry=self.data_registry, dataframe_name=self.model.name)


# Create a registry for Transform implementations
class TransformRegistry(DecoratorRegistrySingleton[str, TransformAbstract]):
    """
    Registry for Transform implementations.

    Maps function names to concrete TransformAbstract implementations.
    """

    pass
