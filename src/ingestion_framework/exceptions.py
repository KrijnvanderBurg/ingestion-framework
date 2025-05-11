"""
Custom exceptions for the ingestion framework.

This module defines exception classes used throughout the ingestion framework
to provide detailed error information and appropriate error handling for various
failure scenarios.
"""


class DictKeyError(KeyError):
    """
    A custom exception raised when a key is not found in a dictionary.

    This exception extends the built-in KeyError class to provide more
    informative error messages that include the missing key and all
    available keys in the dictionary.

    Args:
        key : Any
            The key that was not found in the dictionary
        dict_ : dict
            The dictionary in which the key was not found

    Examples:
        >>> d = {"a": 1, "b": 2}
        >>> try:
        ...     d["c"]
        ... except KeyError as e:
        ...     raise DictKeyError("c", d) from e
        DictKeyError: Could not find key 'c', available keys: ['a', 'b']
    """

    def __init__(self, key, dict_) -> None:
        super().__init__(key)
        self.key = key
        self.dict_ = dict_

    def __str__(self) -> str:
        found_keys = ", ".join(repr(k) for k in self.dict_.keys())
        return f"Could not find key '{self.key}', available keys: [{found_keys}]"
