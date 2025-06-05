from abc import ABC, abstractmethod
from pathlib import Path
import logging
import functools
import polars as pl

# Polars specific into one file for better organization
# -- instead of adding sources to the lib from the lib.
# TODO Move parse csv etc into this
class BaseDomainDataParser(ABC):
    """Abstract base class for domain data parsers returning Polars LazyFrames."""

    SUPPORTED_TYPE: str = ""  # Must be overridden by subclasses

    def __init__(
        self,
        source: str | Path | list[str] | list[Path],
        logger: logging.Logger,
        config: dict[str, any],
    ):
        """
        Initializes the parser with the data source and logger.

        Args:
            source: Path(s) to the data file(s) or database connection info.
            logger: Logger instance to use. Required.
        """
        if not self.SUPPORTED_TYPE:
            raise NotImplementedError(
                f"{self.__class__.__name__} must define SUPPORTED_TYPE"
            )

        if not isinstance(source, str):
            raise TypeError("Source must be a string, Path, list of strings, or list of Paths.")

        if source is None or source == "":
            raise ValueError("Source cannot be None or empty string.")


        self.source = source
        self.logger = logger
        self.config = config

    @abstractmethod
    def _validate_data(self, data_path: Path) -> None:
        pass

    @abstractmethod
    def parse_file(self, **kwargs) -> pl.LazyFrame:
        pass



# Decorator for handling common parsing errors
def handle_parsing_errors(func):
    """Decorator to handle common file/parsing errors for parser methods."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, "logger") or not isinstance(
            self.logger, logging.Logger
        ):
            raise ValueError("Logger not found on parser instance.")

        _logger = self.logger

        source_info = getattr(
            self, "source", "Unknown source"
        )
        parser_name = self.__class__.__name__
        method_name = func.__name__

        _logger.info(
            f"{parser_name}: Attempting to parse source '{source_info}' using {method_name}."
        )
        try:
            result = func(self, *args, **kwargs)
            _logger.info(
                f"{parser_name}: Successfully parsed source '{source_info}'."
            )
            return result
        except (FileNotFoundError, IsADirectoryError) as e:
            _logger.error(
                f"{parser_name}: File/Directory error for source '{source_info}': {e}"
            )
            raise e
        except Exception as e:
            _logger.exception(
                f"{parser_name}: Unexpected error parsing source '{source_info}'",
                exc_info=True,
            )
            raise e

    return wrapper