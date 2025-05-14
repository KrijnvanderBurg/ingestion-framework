"""
Transform functions.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.types import DataFrameRegistrySingleton, Registry, SingletonType
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)

RECIPE: Final[str] = "recipe"
ARGUMENTS: Final[str] = "arguments"


class ArgsAbstract(ABC):
    """
    Abstract base class for the arguments of a transformation function.
    """

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create arguments object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            ArgsAbstract: The arguments object created from the Confeti dictionary.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError


ArgsT = TypeVar("ArgsT", bound=ArgsAbstract)


class RecipeModelAbstract(Generic[ArgsT], ABC):
    """
    Modelification for Transform.

    Args:
        function (str): function name to execute.
        arguments (ArgsT): arguments to pass to the function.
    """

    args_concrete: type[ArgsT]

    def __init__(self, function: str, arguments: ArgsT) -> None:
        """
        Initialize the transformation function.

        Args:
            function (str): The name of the function to execute.
            arguments (ArgsT): The arguments to pass to the function.
        """
        self.function = function
        self.arguments = arguments

    @property
    def function(self) -> str:
        return self._function

    @function.setter
    def function(self, value: str) -> None:
        self._function = value

    @property
    def arguments(self) -> ArgsT:
        return self._arguments

    @arguments.setter
    def arguments(self, value: ArgsT) -> None:
        self._arguments = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation function object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            RecipeModelAbstract: The transformation function object created from the Confeti dictionary.
        """
        try:
            function_name = confeti[RECIPE]
            arguments_dict = confeti[ARGUMENTS]
            arguments = cls.args_concrete.from_confeti(confeti=arguments_dict)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(function=function_name, arguments=arguments)


class RecipeModelPyspark(RecipeModelAbstract[ArgsAbstract], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """


RecipeModelT = TypeVar("RecipeModelT", bound=RecipeModelAbstract)


class RecipeAbstract(Generic[RecipeModelT], ABC):
    """
    Modelification for Transform.

    Args:
        function (str): function name to execute.
        arguments (AbstractArgs): arguments to pass to the function.
    """

    model_concrete: type[RecipeModelT]

    def __init__(self, model: RecipeModelT) -> None:
        """
        Initialize a CastTransform object.

        Args:
            model (CastModel): The CastModel object containing the casting information.
        """
        self.model = model
        self.callable_ = self.transform()

    @property
    def model(self) -> RecipeModelT:
        return self._model

    @model.setter
    def model(self, value: RecipeModelT) -> None:
        self._model = value

    @property
    def callable_(self) -> Callable:
        return self._callable_

    @callable_.setter
    def callable_(self, value: Callable) -> None:
        self._callable_ = value

    @abstractmethod
    def transform(self) -> Callable:
        """TODO"""

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        TODO

        Args:
            confeti (dict[str, Any]): The dictionary.

        Returns:
            RecipeAbstract: model
        """
        model = cls.model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)


class RecipePyspark(RecipeAbstract[RecipeModelT], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """

    @abstractmethod
    def transform(self) -> Callable:
        """TODO"""


class Recipe:
    """
    Base class for all transformation recipes.

    Recipes are reusable transformation components that can be applied to
    dataframes in the ingestion framework.
    """

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> "Recipe":
        """
        Create a recipe instance from configuration.

        Each recipe subclass must implement this method to parse its specific
        configuration parameters.

        Args:
            confeti (dict[str, Any]): The recipe configuration

        Returns:
            Recipe: An initialized recipe instance

        Raises:
            NotImplementedError: If not implemented in a subclass
        """

    def callable_(self, dataframe_registry: "DataFrameRegistrySingleton", dataframe_name: str) -> None:
        """
        Apply the recipe transformation to a dataframe.

        This method must be implemented by each recipe subclass to perform the
        actual transformation logic on the specified dataframe.

        Args:
            dataframe_registry: Registry containing dataframes
            dataframe_name: Name of the dataframe to transform

        Raises:
            NotImplementedError: If not implemented in a subclass
        """


class RecipeRegistrySingleton(Registry, metaclass=SingletonType):
    """
    A singleton registry specifically for transformation recipes.

    This class combines the functionality of the Registry class with a singleton pattern
    implemented via the SingletonType metaclass. It ensures that only one instance of the
    recipe registry exists throughout the application lifecycle.

    This is separate from the DataFrame registry to prevent collisions between recipe and
    dataframe names.

    Inherits:
        Registry: Base registry functionality
        metaclass=SingletonType: Metaclass that implements the singleton pattern
    """

    def register(self, name: str):
        """
        Decorator to register a recipe class with the registry.

        Args:
            name (str): The name under which to register the recipe

        Returns:
            Callable: A decorator that registers the recipe class
        """

        def decorator(recipe_cls):
            self[name] = recipe_cls
            logger.info("Registered recipe '%s': %s", name, recipe_cls.__name__)
            return recipe_cls

        return decorator

    def create_recipe(self, confeti: dict[str, Any]) -> Recipe:
        """
        Create a recipe from configuration.

        Args:
            confeti (dict[str, Any]): The recipe configuration

        Returns:
            Recipe: The created recipe instance

        Raises:
            KeyError: If the recipe name is not found
        """
        recipe_name = confeti.get("recipe")
        if not recipe_name:
            logger.error(f"Missing 'recipe' key in configuration: {confeti}")
            raise KeyError("Missing 'recipe' key in configuration")

        if recipe_name not in self:
            logger.error(f"Recipe '{recipe_name}' not found in registry. Available recipes: {list(self.keys())}")
            raise KeyError(f"Recipe '{recipe_name}' not found in registry")

        recipe_cls = self[recipe_name]
        logger.info(f"Creating recipe '{recipe_name}' with class {recipe_cls.__name__}")
        return recipe_cls.from_confeti(confeti)


recipe_registry = RecipeRegistrySingleton()
