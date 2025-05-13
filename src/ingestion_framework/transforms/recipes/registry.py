"""
Registry for transformation recipes.

This module provides a registry pattern implementation for recipe classes,
allowing for dynamic discovery and instantiation of recipes based on their names.
"""

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Type

from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class Recipe(ABC):
    """Base class for all transformation recipes."""

    recipe_name: ClassVar[str]

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> "Recipe":
        """
        Create a recipe instance from a configuration dictionary.
        """
        raise NotImplementedError("Subclasses must implement from_confeti")

    @abstractmethod
    def callable_(self, dataframe_registry: Any, dataframe_name: str) -> None:
        """
        Apply the recipe transformation to a dataframe.
        """
        raise NotImplementedError("Subclasses must implement callable_")


class RecipeRegistry:
    """Registry for transformation recipes."""

    _recipes: Dict[str, Type[Recipe]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Decorator to register a recipe class.
        """

        def decorator(recipe_cls):
            logger.info(f"Registering recipe: {name} with class {recipe_cls.__name__}")
            cls._recipes[name] = recipe_cls
            recipe_cls.recipe_name = name
            return recipe_cls

        return decorator

    @classmethod
    def get(cls, recipe_name: str) -> Type[Recipe]:
        """
        Get a recipe class by name.
        """
        logger.info(f"Getting recipe: {recipe_name}, available: {list(cls._recipes.keys())}")
        if recipe_name not in cls._recipes:
            logger.error(
                f"Recipe '{recipe_name}' not found in registry. Available recipes: {list(cls._recipes.keys())}"
            )
            raise KeyError(
                f"Recipe '{recipe_name}' not found in registry. Available recipes: {list(cls._recipes.keys())}"
            )

        return cls._recipes[recipe_name]

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Recipe:
        """
        Create a recipe instance from configuration.
        """
        logger.info(f"Creating recipe from confeti: {confeti}")
        recipe_name = confeti.get("recipe", None)
        if not recipe_name:
            logger.error(f"No 'recipe' key found in confeti: {confeti}")
            raise KeyError(f"No 'recipe' key found in confeti: {confeti}")

        recipe_class = cls.get(recipe_name)
        return recipe_class.from_confeti(confeti)


# Create a global recipe registry instance
recipe_registry = RecipeRegistry()
logger.info("Recipe registry initialized")

# Print available recipes at module load time (should be empty at this point)
print(f"Recipe registry initialized. Available recipes: {list(recipe_registry._recipes.keys())}")
