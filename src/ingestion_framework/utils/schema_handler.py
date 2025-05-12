"""
Schema handling utilities for the ingestion framework.

This module provides classes and functions for handling data schemas across
different processing engines, including schema parsing, validation and conversion.
"""

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from pyspark.sql.types import StructType

from ingestion_framework.utils.file_handler import FileHandler, FileHandlerContext
from ingestion_framework.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)

SchemaT = TypeVar("SchemaT", bound=StructType)


class SchemaHandlerAbstract(Generic[SchemaT], ABC):
    """
    Abstract base class for schema handling functionality.

    This class defines the interface for schema handlers, which are responsible
    for creating schemas from various sources.
    """

    @staticmethod
    @abstractmethod
    def schema_factory(schema: str) -> SchemaT:
        """
        Create a schema instance from a schema specification.

        Args:
            schema (str): The schema specification, which could be a JSON string or file path.

        Returns:
            SchemaT: A schema instance of the appropriate type.
        """

    @staticmethod
    @abstractmethod
    def from_json(schema: str) -> SchemaT:
        """
        Create a schema instance from a JSON string.

        Args:
            schema (str): The JSON string containing the schema definition.

        Returns:
            SchemaT: A schema instance of the appropriate type.
        """

    @staticmethod
    @abstractmethod
    def from_file(filepath: str) -> SchemaT:
        """
        Create a schema instance from a file.

        Args:
            filepath (str): The path to the file containing the schema definition.

        Returns:
            SchemaT: A schema instance of the appropriate type.
        """


class SchemaHandlerPyspark(SchemaHandlerAbstract):
    """
    Implementation of schema handler for PySpark's StructType.

    This class provides methods to create PySpark StructType schemas from various
    sources like JSON strings and files.
    """

    @staticmethod
    def schema_factory(schema: str) -> StructType | None:
        """
        Get the appropriate schema handler based on the schema specification.

        This method determines the correct way to parse a schema based on the format
        of the schema string (file path or JSON).

        Args:
            schema (str): The schema specification, which could be a JSON string or file path.

        Returns:
            StructType | None: The schema instance, or None if an empty string is provided.

        Raises:
            NotImplementedError: If the schema value is not recognized or not supported.

        Examples:
            >>> schema = '{"fields":[{"name":"name","type":"string"},{"name":"age","type":"integer"}]}'
            >>> schema_handler = SchemaHandlerPyspark.schema_factory(schema)
            >>>
            >>> # From a file
            >>> schema_handler = SchemaHandlerPyspark.schema_factory("/path/to/schema.json")
        """
        if schema == "":
            return None

        supported_extensions: dict[str, Callable[[str], StructType]] = {
            ".json": SchemaHandlerPyspark.from_file,
        }

        for key, callable_ in supported_extensions.items():
            if schema.endswith(key):
                return callable_(schema)

        if FileHandler.is_json(schema):
            return SchemaHandlerPyspark.from_json(schema=schema)

        raise NotImplementedError(f"No schema handling strategy recognised or supported for value: {schema}")

    @staticmethod
    def from_dict(schema: dict[str, Any]) -> StructType:
        """
        Create a StructType schema from a dictionary.

        Args:
            schema (dict[str, Any]): The dictionary representing the schema.

        Returns:
            StructType: The PySpark schema created from the dictionary.

        Raises:
            ValueError: If there's an error converting the dictionary to a schema.

        Examples:
            >>> schema_dict = {
            >>>     "fields": [
            >>>         {"name": "name", "type": "string", "nullable": True},
            >>>         {"name": "age", "type": "integer", "nullable": True}
            >>>     ],
            >>>     "type": "struct"
            >>> }
            >>> schema = SchemaHandlerPyspark.from_dict(schema_dict)
        """
        try:
            return StructType.fromJson(json=schema)
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{schema}': {e}") from e

    @staticmethod
    def from_json(schema: str) -> StructType:
        """
        Create a StructType schema from a JSON string.

        Args:
            schema (str): The JSON string representing the schema.

        Returns:
            StructType: The PySpark schema created from the JSON string.

        Raises:
            ValueError: If there's an error decoding the JSON schema.

        Examples:
            >>> json_schema = '''
            >>> {
            >>>     "fields": [
            >>>         {"name": "name", "type": "string", "nullable": true},
            >>>         {"name": "age", "type": "integer", "nullable": true}
            >>>     ],
            >>>     "type": "struct"
            >>> }
            >>> '''
            >>> schema = SchemaHandlerPyspark.from_json(json_schema)
        """
        try:
            json_content = json.loads(s=schema)
            return SchemaHandlerPyspark.from_dict(schema=json_content)
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON schema '{schema}': {e}") from e

    @staticmethod
    def from_file(filepath: str) -> StructType:
        """
        Create a StructType schema from a file.

        Args:
            filepath (str): The path to the file containing the schema definition.

        Returns:
            StructType: The PySpark schema created from the file.

        Raises:
            ValueError: If there's an error reading or parsing the file.
            FileNotFoundError: If the specified file doesn't exist.

        Examples:
            >>> schema = SchemaHandlerPyspark.from_file("/path/to/schema.json")
        """
        file_handler = FileHandlerContext.factory(filepath=filepath)
        try:
            schema_dict = file_handler.read()
            return SchemaHandlerPyspark.from_dict(schema=schema_dict)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Schema file not found: {filepath}") from e
        except Exception as e:
            raise ValueError(f"Error reading schema from file {filepath}: {e}") from e
