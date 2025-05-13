"""
Base classes for transform operations in the ingestion framework.

==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from abc import ABC
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.transforms.recipes.registry import Recipe, recipe_registry
from ingestion_framework.types import DataFrameT, RegistrySingleton
from ingestion_framework.utils.log_handler import set_logger

logger = set_logger(__name__)

# Constants
# NOTE: Changed from "functions" to match the test case using "recipes"
FUNCTIONS: Final[str] = "recipes"
FUNCTION: Final[str] = "recipe"
ARGUMENTS: Final[str] = "arguments"
NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"


class TransformModelAbstract(ABC):
    """
    Modelification for data transformation.

    Args:
        name (str): The ID of the transformation modelification.
        recipes (list): List of transformation recipes.
    """

    def __init__(self, name: str, upstream_name: str) -> None:
        self.name = name
        self.upstream_name = upstream_name

    @property
    def name(self) -> str:
        """
        Returns:
            str
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Args:
            value (str)
        """
        self._name = value

    @property
    def upstream_name(self) -> str:
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        self._upstream_name = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a TransformModelAbstract object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            TransformModelAbstract: The TransformModelAbstract object created from the Confeti dictionary.

        Example:
            >>> "transforms": [
            >>>     {
            >>>         "name": "bronze-test-transform-dev",
            >>>         "upstream_name": ["bronze-test-extract-dev"],
            >>>         "functions": [
            >>>             {"recipe": "select_columns", "arguments": {"columns": ["name", "age"]}},
            >>>             // etc.
            >>>         ],
            >>>     }
            >>> ],
        """
        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(name=name, upstream_name=upstream_name)


class TransformModelPyspark(TransformModelAbstract):
    """
    Modelification for PySpark data transformation.
    """

    pass


TransformModelT = TypeVar("TransformModelT", bound=TransformModelAbstract)


class TransformAbstract(Generic[TransformModelT, DataFrameT], ABC):
    """Transform abstract class."""

    load_model_concrete: type[TransformModelT]

    def __init__(self, model: TransformModelT, recipes: list[Recipe]) -> None:
        self.model = model
        self.recipes = recipes
        self.data_registry = RegistrySingleton()

    @property
    def model(self) -> TransformModelT:
        return self._model

    @model.setter
    def model(self, value: TransformModelT) -> None:
        self._model = value

    @property
    def recipes(self) -> list[Recipe]:
        return self._recipes

    @recipes.setter
    def recipes(self, value: list[Recipe]) -> None:
        self._recipes = value

    @property
    def data_registry(self) -> RegistrySingleton:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: RegistrySingleton) -> None:
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """Create an instance of TransformAbstract from configuration."""
        model: TransformModelT = cls.load_model_concrete.from_confeti(confeti=confeti)

        recipes = []
        logger.info(f"Processing transform confeti: {confeti}")
        logger.info(f"Looking for recipes under key '{FUNCTIONS}'")

        for function_confeti in confeti.get(FUNCTIONS, []):
            logger.info(f"Processing recipe confeti: {function_confeti}")
            try:
                recipe = recipe_registry.from_confeti(function_confeti)
                recipes.append(recipe)
                logger.info(f"Successfully created recipe: {recipe.__class__.__name__}")
            except Exception as e:
                logger.error(f"Error creating recipe: {e}")
                raise

        logger.info(f"Created {len(recipes)} recipes")
        return cls(model=model, recipes=recipes)

    def transform(self) -> None:
        """
        Apply all transform recipes on df.
        """
        logger.info(f"Applying {len(self.recipes)} recipes to {self.model.name}")
        for recipe in self.recipes:
            logger.info(f"Applying recipe {recipe.__class__.__name__}")
            recipe.callable_(dataframe_registry=self.data_registry, dataframe_name=self.model.name)
        logger.info(f"All recipes applied to {self.model.name}")


class TransformPyspark(TransformAbstract[TransformModelPyspark, DataFramePyspark]):
    """
    Concrete implementation for PySpark DataFrame transformation.
    """

    load_model_concrete = TransformModelPyspark
