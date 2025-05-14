"""
Registry for data loading operations.

This module provides a registry for storing and retrieving load classes
by name, allowing for dynamic selection of loaders based on configuration.
"""

from typing import Any

from ingestion_framework.load.base import LoadAbstract
from ingestion_framework.registry import ComponentRegistry


class LoadRegistry(ComponentRegistry[LoadAbstract]):
    """
    A singleton registry specifically for loading components.
    
    This registry stores load classes and provides methods for creating
    load instances from configuration. It inherits common registry functionality
    from ComponentRegistry.
    """

    def create_load(self, confeti: dict[str, Any]) -> LoadAbstract:
        """
        Create a load instance from configuration.

        Args:
            confeti (dict[str, Any]): The load configuration

        Returns:
            LoadAbstract: The created load instance

        Raises:
            KeyError: If the load format is not found in registry
        """
        return self.create_component(confeti, "Load format", "data_format")


load_registry = LoadRegistry()
