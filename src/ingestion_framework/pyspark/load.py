"""
TODO
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Self

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.load import (
    DATA_FORMAT,
    LOCATION,
    METHOD,
    MODE,
    NAME,
    OPTIONS,
    SCHEMA_LOCATION,
    UPSTREAM_NAME,
    LoadAbstract,
    LoadContextAbstract,
    LoadFileAbstract,
    LoadFormat,
    LoadMethod,
    LoadMode,
    LoadModelAbstract,
    LoadModelFileAbstract,
)
from ingestion_framework.utils.spark_handler import SparkHandler


class LoadModelPyspark(LoadModelAbstract, ABC):
    """
    Modelification of the sink input.

    Args:
        name (str): ID of the sink modelification.
        method (str): Type of sink load mode.
        mode (str): Type of sink mode.
        data_format (str): Format of the sink input.
        location (str): URI that identifies where to load data in the modelified format.
    """

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        location: str,
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            location (str): URI that identifies where to load data in the modelified format.
            schema_location (str): URI that identifies where to load schema.
            options (dict[str, Any]): Options for the sink input.
        """
        super().__init__(name=name, upstream_name=upstream_name, method=method, location=location)
        self.schema_location = schema_location
        self.options = options

    @property
    def schema_location(self) -> str | None:
        return self._schema_location

    @schema_location.setter
    def schema_location(self, value: str | None) -> None:
        self._schema_location = value

    @property
    def options(self) -> dict[str, str]:
        return self._options

    @options.setter
    def options(self, value: dict[str, str]) -> None:
        self._options = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class LoadModelFilePyspark(LoadModelFileAbstract, LoadModelPyspark):
    """TODO"""

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        mode: LoadMode,
        data_format: LoadFormat,
        location: str,
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            mode (LoadMode): Type of sink mode.
            data_format (LoadFormat): Format of the sink input.
            location (str): URI that identifies where to load data in the modelified format.
            schema_location (str): URI that identifies where to load schema.
            options (dict[str, Any]): Options for the sink input.
        """
        super().__init__(
            name=name,
            upstream_name=upstream_name,
            method=method,
            location=location,
            schema_location=schema_location,
            options=options,
        )
        self.mode = mode
        self.data_format = data_format

    @property
    def mode(self) -> LoadMode:
        return self._mode

    @mode.setter
    def mode(self, value: LoadMode) -> None:
        self._mode = value

    @property
    def data_format(self) -> LoadFormat:
        return self._data_format

    @data_format.setter
    def data_format(self, value: LoadFormat) -> None:
        self._data_format = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a LoadModelAbstract object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            LoadModelAbstract: The LoadModelAbstract object created from the Confeti dictionary.
        """

        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
            method = LoadMethod(confeti[METHOD])
            mode = LoadMode(confeti[MODE])
            data_format = LoadFormat(confeti[DATA_FORMAT])
            location = confeti[LOCATION]
            schema_location = confeti.get(SCHEMA_LOCATION, None)
            options = confeti.get(OPTIONS, {})
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(
            name=name,
            upstream_name=upstream_name,
            method=method,
            mode=mode,
            data_format=data_format,
            location=location,
            schema_location=schema_location,
            options=options,
        )


class LoadPyspark(LoadAbstract[LoadModelPyspark, DataFramePyspark, StreamingQueryPyspark], ABC):
    """
    Concrete implementation for PySpark DataFrame loadion.
    """

    # load_model_concrete = LoadModelPyspark

    def _load_schema(self) -> None:
        """
        Write schema to file.
        """
        if self.model.schema_location:
            with open(file=self.model.schema_location, mode="w", encoding="utf-8") as file:
                schema_json = self.data_registry[self.model.name].schema.json()
                schema_dict = json.loads(schema_json)
                json.dump(schema_dict, file)

    def load(self) -> None:
        """
        Main loadion method.
        """
        SparkHandler().add_configs(options=self.model.options)

        if self.model.method == LoadMethod.BATCH:
            self._load_batch()
        elif self.model.method == LoadMethod.STREAMING:
            self.data_registry[self.model.name] = self._load_streaming()
        else:
            raise ValueError(f"Loadion method {self.model.method} is not supported for Pyspark.")

        self._load_schema()


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


class LoadContextPyspark(LoadContextAbstract):
    """
    _summary_

    Args:
        LoadContextAbstract (_type_): _description_

    Raises:
        NotImplementedError: _description_

    Returns:
        _type_: _description_
    """

    strategy: dict[LoadFormat, type[LoadAbstract]] = {
        LoadFormat.PARQUET: LoadFilePyspark,
        LoadFormat.JSON: LoadFilePyspark,
        LoadFormat.CSV: LoadFilePyspark,
    }
