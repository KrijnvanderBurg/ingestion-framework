"""
Abstract base classes for transformation functions.

This module provides the framework for creating and managing data transformation
functions that can be applied to data frames and other structures.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError

FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"


class ArgsAbstract(ABC):
    """
    Abstract base class for the arguments of a transformation function.
    """

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create arguments object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            ArgsAbstract: The arguments object created from the Confeti dictionary.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError


ArgsT = TypeVar("ArgsT", bound=ArgsAbstract)


class FunctionModelAbstract(Generic[ArgsT], ABC):
    """
    Modelification for Transform.

    Args:
        function (str): function name to execute.
        arguments (ArgsT): arguments to pass to the function.
    """

    args_concrete: type[ArgsT]

    def __init__(self, function: str, arguments: ArgsT) -> None:
        """
        Initialize the transformation function.

        Args:
            function (str): The name of the function to execute.
            arguments (ArgsT): The arguments to pass to the function.
        """
        self.function = function
        self.arguments = arguments

    @property
    def function(self) -> str:
        return self._function

    @function.setter
    def function(self, value: str) -> None:
        self._function = value

    @property
    def arguments(self) -> ArgsT:
        return self._arguments

    @arguments.setter
    def arguments(self, value: ArgsT) -> None:
        self._arguments = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation function object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            FunctionModelAbstract: The transformation function object created from the Confeti dictionary.
        """
        try:
            function_name = confeti[FUNCTION]
            arguments_dict = confeti[ARGUMENTS]
            arguments = cls.args_concrete.from_confeti(confeti=arguments_dict)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(function=function_name, arguments=arguments)


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModelAbstract)


class FunctionAbstract(Generic[FunctionModelT], ABC):
    """
    Abstract base class for transformation functions.

    This class defines the interface for all transformation functions in the system.
    Each function has a model that defines its behavior and parameters.

    Args:
        model (FunctionModelT): The function configuration model.
    """

    model_concrete: type[FunctionModelT]

    def __init__(self, model: FunctionModelT) -> None:
        """
        Initialize a function transformation object.

        Args:
            model (FunctionModelT): The model object containing the function configuration.
        """
        self.model = model
        self.callable_ = self.transform()

    @property
    def model(self) -> FunctionModelT:
        return self._model

    @model.setter
    def model(self, value: FunctionModelT) -> None:
        self._model = value

    @property
    def callable_(self) -> Callable[..., Any]:
        return self._callable_

    @callable_.setter
    def callable_(self, value: Callable[..., Any]) -> None:
        self._callable_ = value

    @abstractmethod
    def transform(self) -> Callable[..., Any]:
        """
        Create a callable transformation function based on the model.

        Returns:
            Callable: A function that applies the transformation.
        """

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a function instance from a configuration dictionary.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            FunctionAbstract: A new function instance.
        """
        model = cls.model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)
