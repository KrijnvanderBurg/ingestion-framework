"""
Extract package for the ingestion framework.

This package provides components for extracting data from various sources in the
ingestion framework. It implements abstractions, models, and concrete implementations
for extracting data in different formats and using different methods.
"""

# Import the registry first so it's available for registration
# Import implementations that register themselves with the registry
from ingestion_framework.extract.file_extract import ExtractFilePyspark
from ingestion_framework.extract.registry import extract_registry

__all__ = ["extract_registry"]
