"""
Singleton metaclass implementation for the ingestion framework.

This module provides a metaclass that implements the Singleton pattern,
ensuring that classes using this metaclass only ever have one instance.
"""

import threading
from typing import Generic, TypeVar

_T = TypeVar("_T")


class SingletonType(type, Generic[_T]):
    """
    Metaclass that implements the Singleton design pattern.

    This metaclass ensures that only one instance of a class is created and
    provides thread-safe instantiation through a lock mechanism.

    Attributes:
        _instances (dict): Dictionary holding the singleton instances.
        _lock (threading.Lock): Threading lock for thread-safety.
    """

    _instances: dict["SingletonType[_T]", _T] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> _T:
        """
        Override to implement singleton pattern when creating instances.

        This method ensures that only one instance of a class is created by checking
        if the class already has an instance in the _instances dictionary.

        Args:
            *args: Variable length argument list for the class constructor.
            **kwargs: Arbitrary keyword arguments for the class constructor.

        Returns:
            _T: The singleton instance of the class.
        """
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Singleton(metaclass=SingletonType):
    """
    Base class for implementing the Singleton pattern.

    Classes that inherit from this class will automatically be singletons.

    Examples:
        >>> class MySingleton(Singleton):
        >>>     pass
        >>>
        >>> instance1 = MySingleton()
        >>> instance2 = MySingleton()
        >>> instance1 is instance2  # True
    """
