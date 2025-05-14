"""
Data loading package for the ingestion framework.

This package provides functionality for loading processed data to various destinations,
including file-based storage in different formats. It implements interfaces, models,
and concrete implementations for data loading operations.
"""

# Import the registry first so it's available for registration
# Import implementations that register themselves with the registry
from ingestion_framework.load.file_load import LoadFilePyspark
from ingestion_framework.load.registry import load_registry

__all__ = ["load_registry"]
