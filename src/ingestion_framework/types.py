import threading
from collections.abc import Iterator
from typing import Any, Callable, Dict, Generic, List, Type, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming import StreamingQuery as StreamingQueryPyspark
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


# Type variables for Registry
K = TypeVar("K")  # Key type for both class registry and instance registry
T = TypeVar("T")  # Value type (class being registered in class registry)
V = TypeVar("V")  # Value type for instance registry (often instances of T)


class DecoratorRegistrySingleton(Generic[K, T], metaclass=SingletonType):
    """
    A unified registry that provides both instance item tracking and class decoration capabilities.

    This registry serves two purposes:
    1. As an instance registry to track and manage objects with keys of type K.
    2. As a decorator registry that maps keys of type K to classes of type T.

    The generic parameters allow for type-safe specialization:
    - K: The key type for both registries (class and instance)
    - T: The value type for class registry (the class being registered)
    - V: The value type for instance registry (often instances of T)

    Example:
        class MyRegistry(Registry[str, MyBaseClass, MyBaseClass]):
            pass

        # Class registration
        @MyRegistry.register("key1")
        class MyImplementation(MyBaseClass):
            pass

        # Instance registration
        registry = MyRegistry()
        registry["instance1"] = MyImplementation()
    """

    # Class-level registry for decorator pattern
    _registry: Dict[K, List[Type[T]]] = {}

    def __init__(self) -> None:
        """Initialize the instance registry."""
        self._items: dict[K, T] = dict()

    # Instance registry methods
    def __setitem__(self, name: K, item: T) -> None:
        """Set an item with a given name. Replaces any existing item."""
        self._items[name] = item

    def __getitem__(self, name: K) -> T:
        """Get an item by its name. Raises KeyError if not found."""
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: K) -> None:
        """Delete an item by its name. Raises KeyError if not found."""
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: K) -> bool:
        """Check if an item exists by its name."""
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items tracked."""
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        """Iterate over the items."""
        return iter(self._items.values())

    # Class decorator registry methods
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


class DataFramePysparkRegistry(DecoratorRegistrySingleton[str, DataFramePyspark | StreamingQueryPyspark]):
    pass
