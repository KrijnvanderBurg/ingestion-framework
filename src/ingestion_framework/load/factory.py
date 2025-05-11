"""
Factory classes for data loading operations.

This module provides factory classes that create appropriate data loaders
based on configuration settings, implementing the Factory Method pattern
to support multiple loading strategies and formats.
"""

from abc import ABC
from typing import Any

from ingestion_framework.load.base import DATA_FORMAT, LoadAbstract, LoadFormat
from ingestion_framework.load.pyspark.file_load import LoadFilePyspark


class LoadContextAbstract(ABC):
    """
    Abstract factory for creating data load instances.

    This class defines the interface for factories that create data load instances
    based on the format specified in the configuration.

    Attributes:
        strategy (dict): Dictionary mapping load formats to load classes.
    """

    strategy: dict[LoadFormat, type[LoadAbstract]]

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[LoadAbstract]:
        """
        Create a load instance based on the configuration.

        Args:
            confeti (dict[str, Any]): The configuration dictionary.

        Returns:
            type[LoadAbstract]: The appropriate load class based on the format.

        Raises:
            NotImplementedError: If the specified load format is not supported.

        Examples:
            >>> confeti = {"data_format": "parquet", "name": "load1", ...}
            >>> load_class = LoadContextAbstract.factory(confeti)
            >>> load_instance = load_class.from_confeti(confeti)
        """

        load_strategy = LoadFormat(confeti[DATA_FORMAT])

        if load_strategy in cls.strategy.keys():
            return cls.strategy[load_strategy]

        raise NotImplementedError(f"Load format {load_strategy.value} is not supported. {type(load_strategy)}")


class LoadContextPyspark(LoadContextAbstract):
    """
    PySpark-specific factory for creating data load instances.

    This class implements the LoadContextAbstract interface for PySpark,
    providing a strategy map of supported load formats to their implementation classes.

    Attributes:
        strategy (dict): Dictionary mapping load formats to PySpark load classes.
    """

    strategy: dict[LoadFormat, type[LoadAbstract]] = {
        LoadFormat.PARQUET: LoadFilePyspark,
        LoadFormat.JSON: LoadFilePyspark,
        LoadFormat.CSV: LoadFilePyspark,
    }
