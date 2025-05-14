"""
Registry for data extraction operations.

This module provides a registry for storing and retrieving extract classes
by name, allowing for dynamic selection of extractors based on configuration.
"""

from typing import Any

from ingestion_framework.extract.base import ExtractAbstract
from ingestion_framework.registry import ComponentRegistry


class ExtractRegistry(ComponentRegistry[ExtractAbstract]):
    """
    A singleton registry specifically for extraction components.

    This registry stores extract classes and provides methods for creating
    extract instances from configuration.
    """

    def create_extract(self, confeti: dict[str, Any]) -> ExtractAbstract:
        """
        Create an extract instance from configuration.

        Args:
            confeti (dict[str, Any]): The extract configuration

        Returns:
            ExtractAbstract: The created extract instance

        Raises:
            KeyError: If the extract format is not found in registry
        """
        return self.create_component(confeti, "Extract format", "data_format")


extract_registry = ExtractRegistry()
