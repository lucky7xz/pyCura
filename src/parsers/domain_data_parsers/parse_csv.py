import logging
import polars as pl
from pathlib import Path

from .base_parse_domain import BaseDomainDataParser, handle_parsing_errors

class CsvParser(BaseDomainDataParser):
    """Parses CSV files into a Polars LazyFrame."""

    SUPPORTED_TYPE = "csv"

    def __init__(
        self,
        source: str | Path | list[str] | list[Path],
        logger: logging.Logger | None = None,
    ):
        """
        Initializes the CSV parser.

        Args:
            source: Path or list of paths to CSV file(s) or a directory
                    containing CSV files. Supports glob patterns.
            logger: Logger instance from the ParsingManager.
        """
        super().__init__(source, logger=logger)

    @handle_parsing_errors
    def parse(self, **kwargs) -> pl.LazyFrame:
        """
        Parses the CSV file(s) using polars.scan_csv.
        Error handling is managed by the @handle_parsing_errors decorator.

        Args:
            **kwargs: Additional keyword arguments to pass directly to
                      polars.scan_csv. Eg. infer_schema_length, separator, etc.

        Returns:
            A Polars LazyFrame representing the CSV data.
        """
        self.logger.info(
            f"Parsing CSV source: {self.source} with options: {kwargs}"
        )

        # Default to infer_schema_length=0 if not specified by the user
        # This will result in the schema being not inferred, and the data will
        # be read as strings.
        kwargs.setdefault("infer_schema_length", 0)

        # scan_csv handles single path, list of paths, glob patterns, and directories
        lf = pl.scan_csv(self.source, **kwargs)
        return lf
