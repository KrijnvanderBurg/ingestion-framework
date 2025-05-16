"""
PySpark implementation of the Job class.

This module provides concrete implementations for executing ETL jobs using PySpark.
"""

from ingestion_framework.job import Engine, JobAbstract, JobRegistry
from ingestion_framework.pyspark.extract import ExtractContextPyspark
from ingestion_framework.pyspark.load import LoadContextPyspark
from ingestion_framework.pyspark.transform import TransformPyspark


@JobRegistry.register(Engine.PYSPARK)
class JobPyspark(JobAbstract):
    """
    Concrete implementation of JobAbstract for PySpark.

    This class provides PySpark-specific functionality for executing ETL jobs.
    """

    extract_concrete = ExtractContextPyspark
    transform_concrete = TransformPyspark
    load_concrete = LoadContextPyspark

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        for extract in self.extracts:
            extract.extract()

        for transform in self.transforms:
            transform.transform()

        for load in self.loads:
            load.load()
