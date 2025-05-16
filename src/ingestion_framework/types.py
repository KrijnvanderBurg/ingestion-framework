import threading
from collections.abc import Iterator
from typing import Any, Callable, Dict, Generic, List, Type, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

DataFrameT = TypeVar("DataFrameT", bound=DataFramePyspark)
StreamingQueryT = TypeVar("StreamingQueryT", bound=StreamingQueryPyspark)


class SingletonType(type):
    _instances: dict[type, Any] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> Any:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Registry:
    """A metaclass for tracking and managing any kind of objects."""

    def __init__(self) -> None:
        self._items: dict[str, Any] = dict()

    def __setitem__(self, name: str, item: Any) -> None:
        """Set an item with a given name. Replaces any existing item."""
        self._items[name] = item

    def __getitem__(self, name: str) -> Any:
        """Get an item by its name. Raises KeyError if not found."""
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: str) -> None:
        """Delete an item by its name. Raises KeyError if not found."""
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: str) -> bool:
        """Check if an item exists by its name."""
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items tracked."""
        return len(self._items)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the items."""
        return iter(self._items.values())


class RegistrySingleton(Registry, metaclass=SingletonType):
    pass


class DataFramePysparkRegistry(RegistrySingleton):
    pass


# Enhanced Decorator registry pattern implementation
K = TypeVar("K")  # Key type
T = TypeVar("T")  # Value type (class being registered)


class DecoratorRegistry(Generic[K, T]):
    """
    A decorator registry that maps keys to classes.
    Used to implement a more flexible registry pattern with decorators.

    This is a generic class that can be specialized for specific use cases.
    The K type parameter represents the key type, and the T type parameter
    represents the value type (the class being registered).

    Example:
        class MyRegistry(DecoratorRegistry[str, MyBaseClass]):
            pass

        @MyRegistry.register("key1")
        class MyImplementation(MyBaseClass):
            pass
    """

    _registry: Dict[Any, List[Type]] = {}

    @classmethod
    def register(cls, key: K) -> Callable[[Type[T]], Type[T]]:
        """
        Class decorator to register a class with a specific key in the registry.

        Args:
            key: The key to register the class with

        Returns:
            A decorator function that registers the class and returns it unchanged

        Example:
            @MyRegistry.register(ExtractFormat.CSV)
            class CsvExtractor(BaseExtractor):
                pass
        """

        def decorator(registered_class: Type[T]) -> Type[T]:
            if key not in cls._registry:
                cls._registry[key] = []
            cls._registry[key].append(registered_class)
            return registered_class

        return decorator

    @classmethod
    def get(cls, key: K) -> Type[T]:
        """
        Get the first registered class for a key.

        Args:
            key: The key to look up

        Returns:
            The registered class

        Raises:
            KeyError: If no class is registered for the key
        """
        if key not in cls._registry or not cls._registry[key]:
            raise KeyError(f"No class registered for key: {key}")
        return cls._registry[key][0]

    @classmethod
    def get_all(cls, key: K) -> List[Type[T]]:
        """
        Get all registered classes for a key.

        Args:
            key: The key to look up

        Returns:
            List of registered classes

        Raises:
            KeyError: If no class is registered for the key
        """
        if key not in cls._registry or not cls._registry[key]:
            raise KeyError(f"No class registered for key: {key}")
        return cls._registry[key].copy()

    @classmethod
    def all(cls) -> Dict[K, List[Type[T]]]:
        """
        Get all registered classes.

        Returns:
            Dictionary mapping keys to lists of registered classes
        """
        return {k: v.copy() for k, v in cls._registry.items()}
