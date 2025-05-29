"""
PySpark implementations of transformation functions.

This module provides PySpark-specific implementations for data transformation
functions that can be applied to PySpark DataFrames.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable

from ingestion_framework2.functions import (
    ArgsAbstract,
    FunctionAbstract,
    FunctionModelAbstract,
    FunctionModelT,
)


class FunctionModelPyspark(FunctionModelAbstract[ArgsAbstract], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """


class FunctionPyspark(FunctionAbstract[FunctionModelT], ABC):
    """
    A concrete implementation of transformation functions using PySpark.

    This class serves as a base for all PySpark-specific transformation functions.
    """

    @abstractmethod
    def transform(self) -> Callable:
        """
        Create a callable transformation function for PySpark DataFrames.

        Returns:
            Callable: A function that transforms a PySpark DataFrame.
        """
