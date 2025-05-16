"""
Module to take care of creating a singleton of the execution environment class.
"""

import logging

from pyspark.sql import SparkSession

from ingestion_framework.utils.log_handler import set_logger
from ingestion_framework.utils.singleton_meta import SingletonType

logger: logging.Logger = set_logger(__name__)


class SparkHandler(metaclass=SingletonType):
    # _session: SparkSession

    def __init__(
        self,
        app_name: str = "ingestion_framework",
        options: dict[str, str] | None = None,
    ) -> None:
        builder = SparkSession.Builder().appName(name=app_name)

        if options:
            for key, value in options.items():
                builder = builder.config(key=key, value=value)

        self.session = builder.getOrCreate()

    @property
    def session(self) -> SparkSession:
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        self._session = session

    @session.deleter
    def session(self) -> None:
        self._session.stop()
        del self._session

    def add_configs(self, options: dict[str, str]) -> None:
        for key, value in options.items():
            self.session.conf.set(key=key, value=value)
