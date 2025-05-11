"""
Common type definitions for the ingestion framework.

This module provides type aliases, registry implementations, and other type-related
constructs used throughout the ingestion framework to enhance type safety and enable
consistent data sharing between components.
"""

import threading
from collections.abc import Iterator
from typing import Any, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

DataFrameT = TypeVar("DataFrameT", bound=DataFramePyspark)
StreamingQueryT = TypeVar("StreamingQueryT", bound=StreamingQueryPyspark)


class SingletonType(type):
    """
    A metaclass that implements the Singleton pattern.

    This metaclass ensures that only one instance of a class is created,
    regardless of how many times the class is instantiated. It maintains
    a dictionary of class instances and uses thread-safe locking to handle
    concurrent instantiation attempts.

    Attributes:
        _instances (dict): A dictionary that maps classes to their singleton instances.
        _lock (threading.Lock): A thread lock to ensure thread-safe instance creation.

    Example:
        ```
        class MyClass(metaclass=SingletonType):
            pass

        # These variables will reference the same instance
        a = MyClass()
        b = MyClass()
        assert a is b  # True
        ```
    """

    _instances: dict["SingletonType", Any] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> Any:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Registry:
    """
    A general-purpose registry for storing and retrieving items by name.

    The Registry class provides a dictionary-like interface for storing any type of object
    with string keys. It allows for adding, retrieving, checking existence of, and removing
    items from the registry.

    This registry is primarily used for storing dataframes by name.

    Example:
        >>> registry = Registry()
        >>> registry["model"] = MyModel()
        >>> model = registry["model"]
        >>> "model" in registry  # True
        >>> len(registry)  # 1
        >>> del registry["model"]
    """

    def __init__(self) -> None:
        self._items: dict[str, Any] = dict()

    def __setitem__(self, name: str, item: Any) -> None:
        """Set an item with a given name. Replaces any existing item."""
        self._items[name] = item

    def __getitem__(self, name: str) -> Any:
        """Get an item by its name. Raises KeyError if not found."""
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: str) -> None:
        """Delete an item by its name. Raises KeyError if not found."""
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: str) -> bool:
        """Check if an item exists by its name."""
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items tracked."""
        return len(self._items)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the items."""
        return iter(self._items.values())


class RegistrySingleton(Registry, metaclass=SingletonType):
    """
    A singleton registry class that ensures only one instance of the registry is created.

    This class combines the functionality of the Registry class with a singleton pattern
    implemented via the SingletonType metaclass. It ensures that only one instance of the
    registry exists throughout the application lifecycle.

    Inherits:
        Registry: Base registry functionality
        metaclass=SingletonType: Metaclass that implements the singleton pattern

    Usage:
        registry = RegistrySingleton()  # First instantiation
        another_registry = RegistrySingleton()  # Returns the same instance as `registry`
        assert registry is another_registry  # True
    """
