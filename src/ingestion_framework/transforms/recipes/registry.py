"""
Recipe registry system for transform operations.

This module provides the recipe registry pattern implementation that allows
recipes to be registered and retrieved by name, simplifying the process of
adding new transformation types to the framework.
"""

from typing import Any, Dict, Type

from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)


class Recipe:
    """
    Base class for all transformation recipes.

    Recipes are reusable transformation components that can be applied to
    dataframes in the ingestion framework.
    """

    pass


class RecipeRegistry:
    """
    Registry for transformation recipes.

    This class maintains a mapping of recipe names to recipe classes,
    allowing recipes to be looked up by name at runtime.
    """

    _registry: Dict[str, Type[Recipe]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Decorator to register a recipe class with the registry.

        Args:
            name (str): The name under which to register the recipe

        Returns:
            Callable: A decorator that registers the recipe class
        """

        def decorator(recipe_cls):
            cls._registry[name] = recipe_cls
            logger.info(f"Registered recipe '{name}': {recipe_cls.__name__}")
            return recipe_cls

        return decorator

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Recipe:
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

        if recipe_name not in cls._registry:
            logger.error(
                f"Recipe '{recipe_name}' not found in registry. Available recipes: {list(cls._registry.keys())}"
            )
            raise KeyError(f"Recipe '{recipe_name}' not found in registry")

        recipe_cls = cls._registry[recipe_name]
        logger.info(f"Creating recipe '{recipe_name}' with class {recipe_cls.__name__}")
        return recipe_cls.from_confeti(confeti)


recipe_registry = RecipeRegistry()
