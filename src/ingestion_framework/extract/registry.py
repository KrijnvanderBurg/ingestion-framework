"""
Registry for data extraction operations.

This module provides a registry for storing and retrieving extract classes
by name, allowing for dynamic selection of extractors based on configuration.
"""

from typing import Any

from ingestion_framework.extract.base import ExtractAbstract
from ingestion_framework.types import Registry, SingletonType
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class ExtractRegistrySingleton(Registry, metaclass=SingletonType):
    """
    A singleton registry specifically for extraction components.

    This class combines the functionality of the Registry class with a singleton pattern
    implemented via the SingletonType metaclass. It ensures that only one instance of the
    extract registry exists throughout the application lifecycle.

    This is separate from other registries to prevent collisions between components.

    Inherits:
        Registry: Base registry functionality
        metaclass=SingletonType: Metaclass that implements the singleton pattern
    """

    def register(self, name: str):
        """
        Decorator to register an extract class with the registry.

        Args:
            name (str): The name under which to register the extract class

        Returns:
            Callable: A decorator that registers the extract class
        """

        def decorator(extract_cls):
            self[name] = extract_cls
            logger.info("Registered extract '%s': %s", name, extract_cls.__name__)
            return extract_cls

        return decorator

    def create_extract(self, confeti: dict[str, Any]) -> ExtractAbstract:
        """
        Create an extract instance from configuration.

        Args:
            confeti (dict[str, Any]): The extract configuration

        Returns:
            ExtractAbstract: The created extract instance

        Raises:
            KeyError: If the extract format or method is not found in registry
        """
        extract_format = confeti.get("data_format")
        if not extract_format:
            logger.error(f"Missing 'data_format' key in configuration: {confeti}")
            raise KeyError("Missing 'data_format' key in configuration")

        extract_key = extract_format

        if extract_key not in self:
            logger.error(
                f"Extract format '{extract_key}' not found in registry. Available formats: {list(self.keys())}"
            )
            raise KeyError(f"Extract format '{extract_key}' not found in registry")

        extract_cls = self[extract_key]
        logger.info(f"Creating extract for format '{extract_format}' with class {extract_cls.__name__}")
        return extract_cls.from_confeti(confeti)


extract_registry = ExtractRegistrySingleton()
