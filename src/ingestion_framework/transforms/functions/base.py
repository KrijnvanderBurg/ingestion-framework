"""
Base classes for transformation functions.

This module provides abstract and concrete base classes for defining
transformation functions that can be applied to data within the ingestion framework.
These functions serve as the building blocks for constructing more complex
transformation pipelines.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError

FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"


class ArgsAbstract(ABC):
    """
    Abstract base class for transformation function arguments.

    This class serves as the base for all argument classes used by
    transformation functions.
    """

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create arguments object from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            ArgsAbstract: The arguments object created from the configuration.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError


ArgsT = TypeVar("ArgsT", bound=ArgsAbstract)


class FunctionModelAbstract(Generic[ArgsT], ABC):
    """
    Abstract base class for transformation function models.

    This class represents the configuration of a transformation function,
    including the function name and arguments.

    Attributes:
        function (str): The name of the function to execute.
        arguments (ArgsT): The arguments to pass to the function.
    """

    args_concrete: type[ArgsT]

    def __init__(self, function: str, arguments: ArgsT) -> None:
        """
        Initialize a transformation function model.

        Args:
            function (str): The name of the function to execute.
            arguments (ArgsT): The arguments to pass to the function.
        """
        self.function = function
        self.arguments = arguments

    @property
    def function(self) -> str:
        """
        Get the function name.

        Returns:
            str: The function name.
        """
        return self._function

    @function.setter
    def function(self, value: str) -> None:
        """
        Set the function name.

        Args:
            value (str): The function name to set.
        """
        self._function = value

    @property
    def arguments(self) -> ArgsT:
        """
        Get the function arguments.

        Returns:
            ArgsT: The function arguments.
        """
        return self._arguments

    @arguments.setter
    def arguments(self, value: ArgsT) -> None:
        """
        Set the function arguments.

        Args:
            value (ArgsT): The function arguments to set.
        """
        self._arguments = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a function model from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            FunctionModelAbstract: The function model created from the configuration.

        Raises:
            DictKeyError: If a required key is missing from the configuration.

        Examples:
            >>> confeti = {
            >>>     "function": "select",
            >>>     "arguments": {"columns": ["name", "age"]}
            >>> }
            >>> model = FunctionModelAbstract.from_confeti(confeti)
        """
        try:
            function_name = confeti[FUNCTION]
            arguments_dict = confeti[ARGUMENTS]
            arguments = cls.args_concrete.from_confeti(confeti=arguments_dict)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(function=function_name, arguments=arguments)


class FunctionModelPyspark(FunctionModelAbstract[ArgsAbstract], ABC):
    """
    PySpark-specific implementation of function model.

    This class extends FunctionModelAbstract for PySpark-specific transformations.
    """


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModelAbstract)


class FunctionAbstract(Generic[FunctionModelT], ABC):
    """
    Abstract base class for transformation functions.

    This class represents a transformation function that can be applied to data.
    It consists of a model that defines the function and a callable that implements
    the actual transformation logic.

    Attributes:
        model (FunctionModelT): The function model.
        callable_ (Callable): The callable that implements the transformation logic.
    """

    model_concrete: type[FunctionModelT]

    def __init__(self, model: FunctionModelT) -> None:
        """
        Initialize a transformation function with a model.

        Args:
            model (FunctionModelT): The function model containing the configuration.

        Examples:
            >>> model = SelectFunctionModel(function="select", arguments=args)
            >>> function = SelectFunction(model=model)
        """
        self.model = model
        self.callable_ = self.transform()

    @property
    def model(self) -> FunctionModelT:
        """
        Get the function model.

        Returns:
            FunctionModelT: The function model.
        """
        return self._model

    @model.setter
    def model(self, value: FunctionModelT) -> None:
        """
        Set the function model.

        Args:
            value (FunctionModelT): The function model to set.
        """
        self._model = value

    @property
    def callable_(self) -> Callable:
        """
        Get the callable that implements the transformation logic.

        Returns:
            Callable: The transformation callable.
        """
        return self._callable_

    @callable_.setter
    def callable_(self, value: Callable) -> None:
        """
        Set the callable that implements the transformation logic.

        Args:
            value (Callable): The transformation callable to set.
        """
        self._callable_ = value

    @abstractmethod
    def transform(self) -> Callable:
        """
        Create a callable that implements the transformation logic.

        Returns:
            Callable: The transformation callable.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation function from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            FunctionAbstract: The transformation function created from the configuration.

        Raises:
            DictKeyError: If a required key is missing from the configuration.

        Examples:
            >>> confeti = {
            >>>     "function": "select",
            >>>     "arguments": {"columns": ["name", "age"]}
            >>> }
            >>> function = FunctionAbstract.from_confeti(confeti)
        """
        model = cls.model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)


class FunctionPyspark(FunctionAbstract[FunctionModelT], ABC):
    """
    PySpark-specific implementation of transformation function.

    This class extends FunctionAbstract for PySpark-specific transformations.
    """

    @abstractmethod
    def transform(self) -> Callable:
        """
        Create a callable that implements the transformation logic for PySpark.

        Returns:
            Callable: The transformation callable.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
