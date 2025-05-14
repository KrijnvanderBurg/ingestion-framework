"""
Registry for data loading operations.

This module provides a registry for storing and retrieving load classes
by name, allowing for dynamic selection of loaders based on configuration.
"""

from typing import Any

from ingestion_framework.load.base import LoadAbstract
from ingestion_framework.types import RegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class LoadRegistry(RegistrySingleton):
    """
    A singleton registry specifically for loading components.

    This registry stores load classes and provides methods for creating
    load instances from configuration. It inherits common registry functionality
    from RegistrySingleton.
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
        component_id = confeti.get("data_format")
        if not component_id:
            raise KeyError("Missing 'data_format' key in configuration")

        if component_id not in self:
            logger.error(f"Load format '{component_id}' not found in registry. Available: {list(self.keys())}")
            raise KeyError(f"Load format '{component_id}' not found in registry")

        component_cls = self[component_id]
        logger.info(f"Creating Load format '{component_id}' with class {component_cls.__name__}")
        return component_cls.from_confeti(confeti)


load_registry = LoadRegistry()
