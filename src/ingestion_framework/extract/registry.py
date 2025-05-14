"""
Registry for data extraction operations.

This module provides a registry for storing and retrieving extract classes
by name, allowing for dynamic selection of extractors based on configuration.
"""

from typing import Any

from ingestion_framework.extract.base import ExtractAbstract
from ingestion_framework.types import RegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class ExtractRegistry(RegistrySingleton):
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
        component_id = confeti.get("data_format")
        if not component_id:
            raise KeyError("Missing 'data_format' key in configuration")

        if component_id not in self:
            logger.error(f"Extract format '{component_id}' not found in registry. Available: {list(self.keys())}")
            raise KeyError(f"Extract format '{component_id}' not found in registry")

        component_cls = self[component_id]
        logger.info(f"Creating Extract format '{component_id}' with class {component_cls.__name__}")
        return component_cls.from_confeti(confeti)


extract_registry = ExtractRegistry()
