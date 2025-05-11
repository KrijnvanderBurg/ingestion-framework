"""
Factory classes for data extraction operations.

This module provides factory classes that create appropriate data extractors
based on configuration settings, implementing the Factory Method pattern
to support multiple extraction strategies and formats.
"""

from abc import ABC
from typing import Any

from ingestion_framework.extract.base import DATA_FORMAT, ExtractAbstract, ExtractFormat
from ingestion_framework.extract.pyspark.file_extract import ExtractFilePyspark


class ExtractContextAbstract(ABC):
    """
    Abstract factory for creating data extract instances.

    This class defines the interface for factories that create data extraction instances
    based on the format specified in the configuration.

    Attributes:
        strategy (dict): Dictionary mapping extract formats to extract classes.
    """

    strategy: dict[ExtractFormat, type[ExtractAbstract]]

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[ExtractAbstract]:
        """
        Create an extract instance based on the configuration.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            type[ExtractAbstract]: The appropriate extract class based on the format.

        Raises:
            NotImplementedError: If the specified extract format is not supported.

        Examples:
            >>> confeti = {"data_format": "parquet", "name": "extract1", ...}
            >>> extract_class = ExtractContextAbstract.factory(confeti)
            >>> extract_instance = extract_class.from_confeti(confeti)
        """

        extract_strategy = ExtractFormat(confeti[DATA_FORMAT])

        if extract_strategy not in cls.strategy.keys():
            raise NotImplementedError(f"Extract format {extract_strategy.value} is not supported.")

        return cls.strategy[extract_strategy]


class ExtractContextPyspark(ExtractContextAbstract):
    """
    PySpark-specific factory for creating data extract instances.

    This class implements the ExtractContextAbstract interface for PySpark,
    providing a strategy map of supported extraction formats to their implementation classes.

    Attributes:
        strategy (dict): Dictionary mapping extract formats to PySpark extract classes.
    """

    strategy: dict[ExtractFormat, type[ExtractAbstract[Any, Any]]] = {
        ExtractFormat.PARQUET: ExtractFilePyspark,
        ExtractFormat.JSON: ExtractFilePyspark,
        ExtractFormat.CSV: ExtractFilePyspark,
    }
