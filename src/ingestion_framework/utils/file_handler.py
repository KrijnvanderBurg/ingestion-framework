"""
File handling utilities for the ingestion framework.

This module provides classes and functions for handling various file operations
and formats, including reading from and writing to different file types.
"""

import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml


class FileHandlerStrategy(ABC):
    """
    Abstract base class for file handling strategies.

    This class defines the interface that all concrete file handling strategies
    must implement.

    Attributes:
        filepath (Path): The path to the file being handled.
    """

    def __init__(self, filepath: str) -> None:
        """
        Initialize the file handling strategy with a file path.

        Args:
            filepath (str): The path to the file to be handled.

        Examples:
            >>> strategy = FileHandlerStrategy("path/to/file.json")
        """
        self.filepath: Path = Path(filepath)

    @abstractmethod
    def read(self) -> dict[str, Any]:
        """
        Read the file and return its contents as a dictionary.

        This method must be implemented by all concrete file handling strategies.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            Exception: Any other error that may occur during file reading.
        """
        raise NotImplementedError

    def _file_exists(self) -> bool:
        """
        Check if the file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return self.filepath.exists()


class FileHandler:
    """
    Main file handler class using the Strategy pattern.

    This class delegates file operations to the appropriate strategy based on
    the file format.

    Attributes:
        strategy (FileHandlerStrategy): The strategy used for handling the file.
    """

    def __init__(self, strategy: FileHandlerStrategy) -> None:
        """
        Initialize FileHandler with a specific strategy.

        Args:
            strategy (FileHandlerStrategy): The strategy to be used for file handling.

        Examples:
            >>> json_strategy = FileJsonHandler("path/to/file.json")
            >>> handler = FileHandler(json_strategy)
        """
        self.strategy: FileHandlerStrategy = strategy

    def set_strategy(self, strategy: FileHandlerStrategy) -> None:
        """
        Change the file handling strategy.

        Args:
            strategy (FileHandlerStrategy): The new strategy to be used.

        Examples:
            >>> handler = FileHandler(json_strategy)
            >>> yaml_strategy = FileYamlHandler("path/to/file.yaml")
            >>> handler.set_strategy(yaml_strategy)
        """
        self.strategy = strategy

    def read(self) -> dict[str, Any]:
        """
        Read the file using the current strategy.

        This method delegates the file reading operation to the current strategy.

        Returns:
            dict[str, Any]: The contents of the file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied.
            ValueError: If the file cannot be parsed.

        Examples:
            >>> handler = FileHandler(json_strategy)
            >>> data = handler.read()
            >>> print(data["some_key"])
        """
        return self.strategy.read()

    @staticmethod
    def is_json(s: str) -> bool:
        """
        Check if a string is valid JSON.

        Args:
            s (str): The string to check.

        Returns:
            bool: True if the string is valid JSON, False otherwise.

        Examples:
            >>> is_json = FileHandler.is_json('{"name": "John", "age": 30}')  # True
            >>> is_json = FileHandler.is_json('Not a JSON string')  # False
        """
        try:
            json.loads(s=s)
            return True
        except json.JSONDecodeError:
            return False


class FileYamlHandler(FileHandlerStrategy):
    """
    Strategy for handling YAML files.

    This class implements the FileHandlerStrategy interface for YAML files.
    """

    def __init__(self, filepath: str) -> None:
        """
        Initialize the YAML file handler with a file path.

        Args:
            filepath (str): The path to the YAML file.
        """
        super().__init__(filepath=filepath)

    def read(self) -> dict[str, Any]:
        """
        Read a YAML file and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the YAML file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            yaml.YAMLError: If there is an error parsing the YAML file.

        Examples:
            >>> yaml_handler = FileYamlHandler("config.yaml")
            >>> data = yaml_handler.read()
            >>> print(data["configuration"]["setting"])
        """
        if not self._file_exists():
            raise FileNotFoundError(f"File '{self.filepath}' not found.")

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error reading YAML file '{self.filepath}': {e}") from e


class FileJsonHandler(FileHandlerStrategy):
    """
    Strategy for handling JSON files.

    This class implements the FileHandlerStrategy interface for JSON files.
    """

    def __init__(self, filepath: str) -> None:
        """
        Initialize the JSON file handler with a file path.

        Args:
            filepath (str): The path to the JSON file.
        """
        super().__init__(filepath=filepath)

    def read(self) -> dict[str, Any]:
        """
        Read a JSON file and return its contents as a dictionary.

        Returns:
            dict[str, Any]: The contents of the JSON file as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied for accessing the file.
            ValueError: If the JSON file cannot be parsed.

        Examples:
            >>> json_handler = FileJsonHandler("config.json")
            >>> data = json_handler.read()
            >>> print(data["configuration"]["setting"])
        """
        if not self._file_exists():
            raise FileNotFoundError(f"File '{self.filepath}' not found.")

        try:
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File '{self.filepath}' not found.") from e
        except PermissionError as e:
            raise PermissionError(f"Permission denied for file '{self.filepath}'.") from e
        except json.JSONDecodeError as e:
            # Using ValueError instead of JSONDecodeError due to complexity in supplying additional arguments.
            raise ValueError(f"Error decoding JSON file '{self.filepath}': {e}") from e


class FileHandlerContext:
    """
    Context class for managing file handler strategies.

    This class provides factory methods to create the appropriate file handler
    strategy based on file extension.

    Attributes:
        SUPPORTED_EXTENSIONS (dict): Dictionary mapping file extensions to handler classes.
    """

    SUPPORTED_EXTENSIONS: dict[str, Any] = {
        ".yml": FileYamlHandler,
        ".yaml": FileYamlHandler,
        ".json": FileJsonHandler,
    }

    @staticmethod
    def get_strategy(filepath: str) -> FileHandlerStrategy:
        """
        Get the appropriate file handling strategy based on the file extension.

        Args:
            filepath (str): The path to the file.

        Returns:
            FileHandlerStrategy: An instance of the appropriate file handling strategy.

        Raises:
            NotImplementedError: If the file extension is not supported.

        Examples:
            >>> strategy = FileHandlerContext.get_strategy("config.json")  # Returns FileJsonHandler
            >>> strategy = FileHandlerContext.get_strategy("config.yaml")  # Returns FileYamlHandler
        """
        _, file_extension = os.path.splitext(filepath)
        strategy_class = FileHandlerContext.SUPPORTED_EXTENSIONS.get(file_extension)
        if strategy_class is None:
            raise NotImplementedError(f"File extension '{file_extension}' is not supported.")

        return strategy_class(filepath=filepath)

    @classmethod
    def factory(cls, filepath: str) -> FileHandler:
        """
        Create a FileHandler with the appropriate strategy based on the file extension.

        This factory method selects and instantiates the appropriate strategy based on
        the file extension and returns a FileHandler configured with that strategy.

        Args:
            filepath (str): The path to the file.

        Returns:
            FileHandler: A FileHandler instance with the appropriate strategy.

        Raises:
            NotImplementedError: If the file extension is not supported.

        Examples:
            >>> handler = FileHandlerContext.factory("config.json")
            >>> data = handler.read()
        """
        strategy_class = cls.get_strategy(filepath=filepath)
        strategy_instance = strategy_class  # Instantiate the strategy class
        file_handler = FileHandler(strategy=strategy_instance)

        return file_handler
