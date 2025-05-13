"""
Transform recipes package.

This package contains the transformation recipes that can be applied to dataframes
during the transformation phase of the ETL process.
"""

from ingestion_framework.transforms.recipes.registry import Recipe, recipe_registry

# Explicitly import recipe modules to ensure decorators are executed
from ingestion_framework.transforms.recipes.select_columns import SelectColumnsRecipePyspark

__all__ = [
    "Recipe",
    "recipe_registry",
    "SelectColumnsRecipePyspark",
]
