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

    def __iter__(self) -> Iterator[str]:
        """Iterate over the registry's keys."""
        return iter(self._items)

    def keys(self) -> list[str]:
        """Get all keys in the registry."""
        return list(self._items.keys())


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
    
    def register(self, name: str):
        """
        Decorator to register a class with the registry.

        Args:
            name (str): The name under which to register the class

        Returns:
            Callable: A decorator that registers the class
        """
        from ingestion_framework.utils.log_handler import set_logger
        logger = set_logger(__name__)

        def decorator(cls):
            self[name] = cls
            logger.info(f"Registered '{name}': {cls.__name__}")
            return cls

        return decorator

    def create_component(self, confeti: dict[str, Any], component_name: str, key_name: str) -> Any:
        """
        Create a component instance from configuration.

        Args:
            confeti (dict[str, Any]): The configuration dictionary
            component_name (str): The human-readable name of the component type (for error messages)
            key_name (str): The key in the configuration that contains the component identifier

        Returns:
            Any: The created component instance

        Raises:
            KeyError: If the component identifier is not found or not registered
        """
        from ingestion_framework.utils.log_handler import set_logger
        logger = set_logger(__name__)
        
        component_id = confeti.get(key_name)
        if not component_id:
            logger.error(f"Missing '{key_name}' key in configuration: {confeti}")
            raise KeyError(f"Missing '{key_name}' key in configuration")

        if component_id not in self:
            logger.error(
                f"{component_name} '{component_id}' not found in registry. Available: {list(self.keys())}"
            )
            raise KeyError(f"{component_name} '{component_id}' not found in registry")

        component_cls = self[component_id]
        logger.info(f"Creating {component_name} '{component_id}' with class {component_cls.__name__}")
        return component_cls.from_confeti(confeti)


class ComponentRegistrySingleton(RegistrySingleton):
    """
    A base class for component registries that provides common functionality.
    
    This class extends the RegistrySingleton with component-specific registration
    and creation methods.
    """
    
    def register_component(self, name: str, component_class: Any) -> None:
        """
        Register a component class with the registry.
        
        Args:
            name (str): The name under which to register the component
            component_class (Any): The component class to register
        """
        from ingestion_framework.utils.log_handler import set_logger
        logger = set_logger(__name__)
        
        self[name] = component_class
        logger.info(f"Registered component '{name}': {component_class.__name__}")
    
    def create_from_config(self, confeti: dict[str, Any], key_name: str, component_type_name: str) -> Any:
        """
        Create a component from configuration.
        
        Args:
            confeti (dict[str, Any]): The configuration dictionary
            key_name (str): The key in the configuration that identifies the component
            component_type_name (str): The human-readable type name for error messages
            
        Returns:
            Any: The created component instance
            
        Raises:
            KeyError: If the component identifier is not found or not registered
        """
        from ingestion_framework.utils.log_handler import set_logger
        logger = set_logger(__name__)
        
        component_id = confeti.get(key_name)
        if not component_id:
            logger.error(f"Missing '{key_name}' key in configuration: {confeti}")
            raise KeyError(f"Missing '{key_name}' key in configuration")
            
        if component_id not in self:
            logger.error(
                f"{component_type_name} '{component_id}' not found in registry. "
                f"Available: {list(self.keys())}"
            )
            raise KeyError(f"{component_type_name} '{component_id}' not found in registry")
            
        component_cls = self[component_id]
        logger.info(f"Creating {component_type_name} '{component_id}' with class {component_cls.__name__}")
        return component_cls.from_confeti(confeti)


class DataFrameRegistrySingleton(Registry, metaclass=SingletonType):
    """
    A singleton registry specifically for storing and retrieving DataFrames by name.

    This class combines the functionality of the Registry class with a singleton pattern
    implemented via the SingletonType metaclass. It ensures that only one instance of the
    dataframe registry exists throughout the application lifecycle.

    This is separate from the Recipe registry to prevent collisions between recipe and
    dataframe names.

    Inherits:
        Registry: Base registry functionality
        metaclass=SingletonType: Metaclass that implements the singleton pattern

    Usage:
        df_registry = DataFrameRegistrySingleton()  # First instantiation
        another_df_registry = DataFrameRegistrySingleton()  # Returns the same instance
        assert df_registry is another_df_registry  # True

        # Store a DataFrame
        df_registry["my_dataframe"] = my_dataframe

        # Retrieve a DataFrame
        retrieved_df = df_registry["my_dataframe"]
    """
