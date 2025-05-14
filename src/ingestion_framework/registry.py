"""
Centralized registry module for the ingestion framework.

This module provides base classes and utilities for managing registries
across the ingestion framework, reducing code duplication and providing
consistent behavior for different component types.
"""

from typing import Any, Callable, Dict, Generic, Type, TypeVar

from ingestion_framework.types import Registry, SingletonType
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)

T = TypeVar("T")


class ComponentRegistry(Registry, Generic[T], metaclass=SingletonType):
    """
    A base registry for components that provides common functionality.

    This class implements a singleton registry pattern for components that can be
    registered by name and created from configuration. It provides decorators
    for registration and methods for component creation.
    """

    def register(self, name: str) -> Callable[[Type[T]], Type[T]]:
        """
        Decorator to register a component class with the registry.

        Args:
            name (str): The name under which to register the component

        Returns:
            Callable: A decorator function that registers the component

        Example:
            @my_registry.register("my_component")
            class MyComponent:
                pass
        """

        def decorator(cls: Type[T]) -> Type[T]:
            self[name] = cls
            logger.info(f"Registered component '{name}': {cls.__name__}")
            return cls

        return decorator

    def create_component(self, confeti: Dict[str, Any], component_type_name: str, key_name: str) -> T:
        """
        Create a component instance from configuration.

        Args:
            confeti: The configuration dictionary
            component_type_name: Human-readable name of component type for error messages
            key_name: Key in the configuration that identifies the component

        Returns:
            T: The created component instance

        Raises:
            KeyError: If the component identifier is not found or not registered
        """
        component_id = confeti.get(key_name)
        if not component_id:
            logger.error(f"Missing '{key_name}' key in configuration: {confeti}")
            raise KeyError(f"Missing '{key_name}' key in configuration")

        if component_id not in self:
            logger.error(
                f"{component_type_name} '{component_id}' not found in registry. Available: {list(self.keys())}"
            )
            raise KeyError(f"{component_type_name} '{component_id}' not found in registry")

        component_cls = self[component_id]
        logger.info(f"Creating {component_type_name} '{component_id}' with class {component_cls.__name__}")
        return component_cls.from_confeti(confeti)
