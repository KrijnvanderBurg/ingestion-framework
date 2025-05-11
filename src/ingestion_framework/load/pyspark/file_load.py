"""
TODO"""

from abc import ABC
from typing import Generic

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

from ingestion_framework.load.base import LoadAbstract, LoadModelFilePyspark, LoadModelT, LoadPyspark
from ingestion_framework.types import DataFrameT, StreamingQueryT


class LoadFileAbstract(
    LoadAbstract[LoadModelT, DataFrameT, StreamingQueryT], Generic[LoadModelT, DataFrameT, StreamingQueryT], ABC
):
    """
    Abstract class for file loadion.
    """


class LoadFilePyspark(LoadFileAbstract[LoadModelFilePyspark, DataFramePyspark, StreamingQueryPyspark], LoadPyspark):
    """
    Concrete class for file loadion using PySpark DataFrame.
    """

    load_model_concrete = LoadModelFilePyspark

    def _load_batch(self) -> None:
        """
        Write to file in batch mode.
        """
        self.data_registry[self.model.name].write.save(
            path=self.model.location,
            format=self.model.data_format.value,
            mode=self.model.mode.value,
            **self.model.options,
        )

    def _load_streaming(self) -> StreamingQueryPyspark:
        """
        Write to file in streaming mode.

        Returns:
            StreamingQuery: Represents the ongoing streaming query.
        """
        return self.data_registry[self.model.name].writeStream.start(
            path=self.model.location,
            format=self.model.data_format.value,
            outputMode=self.model.mode.value,
            **self.model.options,
        )
