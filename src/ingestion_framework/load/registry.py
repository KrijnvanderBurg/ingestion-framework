"""
Registry for data loading operations.

This module provides a registry for storing and retrieving load classes
by name, allowing for dynamic selection of loaders based on configuration.
"""

from typing import Any

from ingestion_framework.load.base import LoadAbstract
from ingestion_framework.types import Registry, SingletonType
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class LoadRegistrySingleton(Registry, metaclass=SingletonType):
    """
    A singleton registry specifically for loading components.

    This class combines the functionality of the Registry class with a singleton pattern
    implemented via the SingletonType metaclass. It ensures that only one instance of the
    load registry exists throughout the application lifecycle.

    This is separate from other registries to prevent collisions between components.

    Inherits:
        Registry: Base registry functionality
        metaclass=SingletonType: Metaclass that implements the singleton pattern
    """

    def register(self, name: str):
        """
        Decorator to register a load class with the registry.

        Args:
            name (str): The name under which to register the load class

        Returns:
            Callable: A decorator that registers the load class
        """

        def decorator(load_cls):
            self[name] = load_cls
            logger.info("Registered load '%s': %s", name, load_cls.__name__)
            return load_cls

        return decorator

    def create_load(self, confeti: dict[str, Any]) -> LoadAbstract:
        """
        Create a load instance from configuration.

        Args:
            confeti (dict[str, Any]): The load configuration

        Returns:
            LoadAbstract: The created load instance

        Raises:
            KeyError: If the load format is not found
        """
        load_format = confeti.get("data_format")
        if not load_format:
            logger.error(f"Missing 'data_format' key in configuration: {confeti}")
            raise KeyError("Missing 'data_format' key in configuration")

        load_key = load_format

        if load_key not in self:
            logger.error(f"Load format '{load_key}' not found in registry. Available formats: {list(self.keys())}")
            raise KeyError(f"Load format '{load_key}' not found in registry")

        load_cls = self[load_key]
        logger.info(f"Creating load for format '{load_format}' with class {load_cls.__name__}")
        return load_cls.from_confeti(confeti)


load_registry = LoadRegistrySingleton()
