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

    This class defines the interface for all argument containers used by
    transformation functions. Each concrete implementation should provide
    type-specific argument handling.
    """

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create arguments object from a configuration dictionary.

        Args:
            confeti: The configuration dictionary containing argument specifications.

        Returns:
            An initialized arguments object with values from the configuration.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
            ValueError: If argument values are invalid.
        """
        ...


ArgsT = TypeVar("ArgsT", bound=ArgsAbstract)


class FunctionModelAbstract(Generic[ArgsT], ABC):
    """
    Model specification for transformation functions.

    This class represents the configuration for a transformation function,
    including its name and arguments.
    """

    args_concrete: type[ArgsT]

    def __init__(self, function: str, arguments: ArgsT) -> None:
        """
        Initialize the transformation function model.

        Args:
            function: The name of the function to execute.
            arguments: The arguments to pass to the function.
        """
        self.function = function
        self.arguments = arguments

    @property
    def function(self) -> str:
        """Get the function name."""
        return self._function

    @function.setter
    def function(self, value: str) -> None:
        """Set the function name."""
        self._function = value

    @property
    def arguments(self) -> ArgsT:
        """Get the function arguments."""
        return self._arguments

    @arguments.setter
    def arguments(self, value: ArgsT) -> None:
        """Set the function arguments."""
        self._arguments = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation function model from a configuration dictionary.

        Args:
            confeti: The configuration dictionary containing:
                - 'function': The name of the function to execute
                - 'arguments': The arguments specification for the function

        Returns:
            An initialized function model.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
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
    """

    model_concrete: type[FunctionModelT]

    def __init__(self, model: FunctionModelT) -> None:
        """
        Initialize a function transformation object.

        Args:
            model: The model object containing the function configuration.
        """
        self.model = model
        self.callable_ = self.transform()

    @property
    def model(self) -> FunctionModelT:
        """Get the function model configuration."""
        return self._model

    @model.setter
    def model(self, value: FunctionModelT) -> None:
        """Set the function model configuration."""
        self._model = value

    @property
    def callable_(self) -> Callable[..., Any]:
        """Get the callable transformation function."""
        return self._callable_

    @callable_.setter
    def callable_(self, value: Callable[..., Any]) -> None:
        """Set the callable transformation function."""
        self._callable_ = value

    @abstractmethod
    def transform(self) -> Callable[..., Any]:
        """
        Create a callable transformation function based on the model.

        This method should implement the logic to create a function that
        can be called to transform data according to the model configuration.

        Returns:
            A callable function that applies the transformation to data.
        """

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a function instance from a configuration dictionary.

        Args:
            confeti: The configuration dictionary containing function specifications.

        Returns:
            A new function instance configured according to the provided parameters.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
        """
        model = cls.model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)
