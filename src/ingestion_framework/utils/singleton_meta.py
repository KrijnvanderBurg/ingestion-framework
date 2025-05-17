"""
Module providing metaclasses for implementing the Singleton design pattern.
"""

import threading
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class SingletonType(type, Generic[T]):
    """
    Metaclass to ensure only one instance of a class is created.

    Classes using this metaclass will always return the same instance
    when instantiated multiple times.
    """

    _instances: dict[type, Any] = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> T:
        """
        Override the call method to implement the singleton behavior.

        Returns:
            T: The singleton instance of the class.
        """
        with cls._lock:
            if cls not in cls._instances:
                # Create a new instance if one doesn't exist yet
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Singleton(metaclass=SingletonType):
    """
    Base class for creating singleton classes by inheritance.
    """
