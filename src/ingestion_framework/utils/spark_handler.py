"""
Spark session handling utilities for the ingestion framework.

This module provides a singleton-based handler for managing PySpark sessions
and configurations, ensuring consistent access to Spark functionality across
the ingestion framework.
"""

import logging

from pyspark.sql import SparkSession

from ingestion_framework.utils.log_handler import set_logger
from ingestion_framework.utils.singleton_meta import SingletonType

logger: logging.Logger = set_logger(__name__)


class SparkHandler(metaclass=SingletonType):
    """
    SparkSession handling as a singleton.

    This class ensures only one SparkSession exists across the application lifecycle
    by utilizing the SingletonType metaclass. It provides methods to create,
    access, and configure the SparkSession.

    Attributes:
        session (SparkSession): The managed SparkSession instance.
    """

    def __init__(self, app_name: str = "ingestion_framework", options: dict[str, str] | None = None) -> None:
        """
        Initialize the Spark handler with application name and optional configurations.

        Args:
            app_name (str): Name of the Spark application. Defaults to "ingestion_framework".
            options (dict[str, str], optional): Dictionary of Spark configuration options.
                Each key-value pair will be applied as a configuration setting. Defaults to None.

        Examples:
            >>> spark_handler = SparkHandler(app_name="my_app")
            >>> spark_handler = SparkHandler(options={"spark.sql.shuffle.partitions": "10"})
        """
        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                builder = builder.config(key=key, value=value)

        self.session = builder.getOrCreate()

    @property
    def session(self) -> SparkSession:
        """
        Get the current SparkSession instance.

        Returns:
            SparkSession: The current SparkSession instance.
        """
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        """
        Set the SparkSession instance.

        Args:
            session (SparkSession): The SparkSession to use.
        """
        self._session = session

    @session.deleter
    def session(self) -> None:
        """
        Stop and delete the current SparkSession instance.
        """
        self._session.stop()
        del self._session

    def add_configs(self, options: dict[str, str]) -> None:
        """
        Add additional configurations to the existing SparkSession.

        Args:
            options (dict[str, str]): Dictionary of Spark configuration options.
                Each key-value pair will be applied to the current session.

        Examples:
            >>> spark_handler = SparkHandler()
            >>> spark_handler.add_configs({"spark.executor.memory": "4g"})
        """
        for key, value in options.items():
            self.session.conf.set(key=key, value=value)
