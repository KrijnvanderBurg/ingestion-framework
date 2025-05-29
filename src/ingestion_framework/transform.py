"""
 implementation for data transformation operations.

This module provides concrete implementations for transforming data using .
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.select import SelectFunction
from ingestion_framework.types import DataFrameRegistry, RegistryDecorator, Singleton

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


class Args(ABC):
    """
     base class for the arguments of a transformation function.

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


ArgsT = TypeVar("ArgsT", bound=Args)


class FunctionModel(Generic[ArgsT], ABC):
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


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModel)


class Function(Generic[FunctionModelT], ABC):
    """
     base class for transformation functions.

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


FunctionT = TypeVar("FunctionT", bound=Function)


class TransformRegistry(RegistryDecorator, metaclass=Singleton):
    """
    Registry for Extract implementations.

    Maps ExtractFormat enum values to concrete ExtractAbstract implementations.
    """


class TransformModel:
    """
    Modelification for  data transformation.

    Examples:
        >>> df = spark.createDataFrame(data=[("Alice", 27), ("Bob", 32),], schema=["name", "age"])
        >>> dict = {"function": "cast", "arguments": {"columns": {"age": "StringType",}}}
        >>> transform = TransformFunction.from_dict(dict=dict[str, Any])
        >>> df = df.transform(func=transform).printSchema()
        root
        |-- name: string (nullable = true)
        |-- age: string (nullable = true)
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


class Transform(Generic[FunctionT]):
    """
    Concrete implementation for  DataFrame transformation.

    This class provides -specific functionality for transforming data.
    """

    SUPPORTED_FUNCTIONS: dict[str, type[Function[Any]]] = {
        "select": SelectFunction,
    }

    def __init__(self, model: TransformModel, functions: list[FunctionT]) -> None:
        self.model = model
        self.functions = functions
        self.data_registry = DataFrameRegistry()

    @property
    def model(self) -> TransformModel:
        return self._model

    @model.setter
    def model(self, value: TransformModel) -> None:
        self._model = value

    @property
    def functions(self) -> list[FunctionT]:
        return self._functions

    @functions.setter
    def functions(self, value: list[FunctionT]) -> None:
        self._functions = value

    @property
    def data_registry(self) -> DataFrameRegistry:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: DataFrameRegistry) -> None:
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
        model = TransformModel.from_confeti(confeti=confeti)
        functions: list = []

        for function_confeti in confeti.get(FUNCTIONS, []):
            function_name: str = function_confeti[FUNCTION]

            if function_name not in cls.SUPPORTED_FUNCTIONS:
                raise NotImplementedError(f"{FUNCTION} {function_name} is not supported.")

            function_concrete = cls.SUPPORTED_FUNCTIONS[function_name]
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
