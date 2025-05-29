"""
Extract interface and implementations for various data formats.

This module provides abstract classes and implementations for data extraction
from various sources and formats.
"""


class ExtractModelFileAbstract(ExtractModel, ABC):
    """
    Abstract base class for file-based extraction models.

    This class serves as a marker interface for all extraction models
    that read data from file-based sources.
    """


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract class for file extraction.
    """


class ExtractContextAbstract(ABC):
    """
    Abstract context class for creating and managing extraction strategies.

    This class implements the Strategy pattern for data extraction, allowing
    different extraction implementations to be selected based on the data format.
    """

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[ExtractAbstract]:
        """
        Create an appropriate extract class based on the format specified in the configuration.

        This factory method uses the ExtractRegistry to look up the appropriate
        implementation class based on the data format.

        Args:
            confeti: Configuration dictionary that must include a 'data_format' key
                compatible with the ExtractFormat enum

        Returns:
            The concrete extraction class for the specified format

        Raises:
            NotImplementedError: If the specified extract format is not supported
            KeyError: If the 'data_format' key is missing from the configuration
        """
        try:
            extract_format = ExtractFormat(confeti[DATA_FORMAT])
            return ExtractRegistry.get(extract_format)
        except KeyError as e:
            format_name = confeti.get(DATA_FORMAT, "<missing>")
            raise NotImplementedError(f"Extract format {format_name} is not supported.") from e


class ExtractModelFilePyspark(ExtractModelFileAbstract, ExtractModelPyspark):
    """
    Model for file extraction using PySpark.

    This model configures extraction operations for reading files with PySpark,
    including format, location, and schema information.
    """

    def __init__(
        self,
        name: str,
        method: ExtractMethod,
        data_format: ExtractFormat,
        location: str,
        options: dict[str, str],
        schema: StructType | None = None,
    ) -> None:
        """
        Initialize ExtractModelFilePyspark with the specified parameters.

        Args:
            name: Identifier for this extraction operation
            method: Method of extraction (batch or streaming)
            data_format: Format of the files to extract (parquet, json, csv)
            location: URI where the files are located
            options: PySpark reader options as key-value pairs
            schema: Optional schema definition for the data structure
        """
        super().__init__(
            name=name,
            method=method,
            options=options,
            schema=schema,
        )
        self.data_format = data_format
        self.location = location

    @property
    def data_format(self) -> ExtractFormat:
        """Get the format of the files to extract."""
        return self._data_format

    @data_format.setter
    def data_format(self, value: ExtractFormat) -> None:
        """Set the format of the files to extract."""
        self._data_format = value

    @property
    def location(self) -> str:
        """Get the location URI of the files to extract."""
        return self._location

    @location.setter
    def location(self, value: str) -> None:
        """Set the location URI of the files to extract."""
        self._location = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create an ExtractModelFilePyspark object from a configuration dictionary.

        Args:
            confeti: The configuration dictionary containing extraction parameters

        Returns:
            An initialized extraction model for file-based sources

        Raises:
            DictKeyError: If required keys are missing from the configuration
        """
        try:
            name = confeti[NAME]
            method = ExtractMethod(confeti[METHOD])
            data_format = ExtractFormat(confeti[DATA_FORMAT])
            location = confeti[LOCATION]
            options = confeti[OPTIONS]
            schema = SchemaFilepathHandler.parse(schema=confeti[SCHEMA])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti) from e

        return cls(
            name=name,
            method=method,
            data_format=data_format,
            location=location,
            options=options,
            schema=schema,
        )
