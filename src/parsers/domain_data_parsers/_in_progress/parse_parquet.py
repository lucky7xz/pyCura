import polars as pl
from pathlib import Path
import logging
from src.parsers.domain_data_parsers.parsing_manager import BaseDomainDataParser, handle_parsing_errors

class ParquetParser(BaseDomainDataParser):
    """Parses Parquet files into a Polars LazyFrame."""
    SUPPORTED_TYPE = 'parquet'

    def __init__(self, source: str | Path | list[str] | list[Path], logger: logging.Logger | None = None):
        """
        Initializes the Parquet parser.

        Args:
            source: Path or list of paths to Parquet file(s) or a directory
                    containing Parquet files. Supports glob patterns.
            logger: Logger instance.
        """
        super().__init__(source, logger=logger)

    @handle_parsing_errors
    def parse(self, **kwargs) -> pl.LazyFrame:
        """
        Parses the Parquet file(s) using polars.scan_parquet.
        Error handling is managed by the @handle_parsing_errors decorator.

        Args:
            **kwargs: Additional keyword arguments to pass directly to
                      polars.scan_parquet.

        Returns:
            A Polars LazyFrame representing the Parquet data.
        """
        self.logger.info(f"Parsing Parquet source: {self.source} with options: {kwargs}")
        # scan_parquet handles single path, list of paths, glob patterns, and directories
        lf = pl.scan_parquet(self.source, **kwargs)
        return lf