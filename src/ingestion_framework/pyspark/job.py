"""
Job class.
"""

from ingestion_framework.job import JobAbstract
from ingestion_framework.pyspark.extract import ExtractContextPyspark
from ingestion_framework.pyspark.load import LoadContextPyspark
from ingestion_framework.pyspark.transform import TransformPyspark


class JobPyspark(JobAbstract):
    """
    TODO
    """

    extract_concrete = ExtractContextPyspark
    transform_concrete = TransformPyspark
    load_concrete = LoadContextPyspark

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        self._extract()
        self._transform()
        self._load()
