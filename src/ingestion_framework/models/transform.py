"""
 implementation for data transformation operations.

This module provides concrete implementations for transforming data using .
"""

from abc import ABC
from dataclasses import dataclass
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError

from . import Model

FUNCTIONS: Final[str] = "functions"
FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


@dataclass
class ArgsModel(Model, ABC):
    """
     base class for the arguments of a transformation function.

    This class defines the interface for all argument containers used by
    transformation functions. Each concrete implementation should provide
    type-specific argument handling.
    """


ArgsT = TypeVar("ArgsT", bound=ArgsModel)


class FunctionModel(Model, Generic[ArgsT], ABC):
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

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a transformation function model from a configuration dictionary.

        Args:
            dict_: The configuration dictionary containing:
                - 'function': The name of the function to execute
                - 'arguments': The arguments specification for the function

        Returns:
            An initialized function model.

        Raises:
            DictKeyError: If required keys are missing from the configuration.
        """
        try:
            function_name = dict_[FUNCTION]
            arguments_dict = dict_[ARGUMENTS]
            arguments = cls.args_concrete.from_dict(dict_=arguments_dict)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(function=function_name, arguments=arguments)


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModel)


class TransformModel(Model):
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

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Create a TransformModelAbstract object from a dict_ dictionary.

        Args:
            dict_ (dict[str, Any]): The dict_ dictionary.

        Returns:
            TransformModelAbstract: The TransformModelAbstract object created from the dict_ dictionary.

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
            name = dict_[NAME]
            upstream_name = dict_[UPSTREAM_NAME]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(name=name, upstream_name=upstream_name)
