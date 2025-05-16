"""
Transform functions.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable

from ingestion_framework.functions import ArgsAbstract, FunctionAbstract, FunctionModelAbstract, FunctionModelT


class FunctionModelPyspark(FunctionModelAbstract[ArgsAbstract], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """


class FunctionPyspark(FunctionAbstract[FunctionModelT], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """

    @abstractmethod
    def transform(self) -> Callable:
        """TODO"""
