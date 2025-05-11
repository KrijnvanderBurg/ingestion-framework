import threading
from typing import Generic, TypeVar

_T = TypeVar("_T")


class SingletonType(type, Generic[_T]):
    _instances: dict["SingletonType[_T]", _T] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> _T:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Singleton(metaclass=SingletonType):
    pass
