"""
Recipe registry system for transform operations.

This module provides the recipe registry pattern implementation that allows
recipes to be registered and retrieved by name, simplifying the process of
adding new transformation types to the framework.
"""

from typing import TYPE_CHECKING, Any

from ingestion_framework.types import Registry, SingletonType

if TYPE_CHECKING:
    from ingestion_framework.types import DataFrameRegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


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
        raise NotImplementedError("Recipe subclasses must implement from_confeti")

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
        raise NotImplementedError("Recipe subclasses must implement callable_")


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
            logger.info(f"Registered recipe '{name}': {recipe_cls.__name__}")
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


# Create a reference to the class for better code clarity but don't instantiate it globally
recipe_registry = RecipeRegistrySingleton
