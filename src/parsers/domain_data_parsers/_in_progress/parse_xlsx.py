import polars as pl
from pathlib import Path
import logging
from src.parsers.domain_data_parsers.parsing_manager import BaseDomainDataParser, handle_parsing_errors

class XlsxParser(BaseDomainDataParser):
    """Parses XLSX (Excel) files into a Polars LazyFrame."""
    SUPPORTED_TYPE = 'xlsx'

    def __init__(self, source: str | Path, logger: logging.Logger | None = None):
        """
        Initializes the XLSX parser.

        Args:
            source: Path to the XLSX file. Polars' read_excel typically
                    handles only single files directly, not lists or globs.
                    The source can also be bytes or a file-like object.
            logger: Logger instance.
        """
        if isinstance(source, list):
            # Cannot log here yet
            raise ValueError("XlsxParser source currently supports only a single file path/buffer, not a list.")
        super().__init__(source, logger=logger)

    @handle_parsing_errors
    def parse(self, sheet_name: str | int | None = None, **kwargs) -> pl.LazyFrame:
        """
        Parses the specified sheet from the Excel file(s).
        Error handling is managed by the @handle_parsing_errors decorator.

        Args:
            sheet_name: Name or index (0-based) of the sheet to read. If None,
                        reads the first sheet. Defaults to None.
            **kwargs: Additional keyword arguments passed directly to
                      polars.read_excel (e.g., read_csv_options={'has_header': False},
                      engine='calamine', schema_overrides={...}).

        Returns:
            A Polars LazyFrame representing the Excel sheet data.

        Raises:
            FileNotFoundError: If the source file(s) don't exist.
            IsADirectoryError: If the source path points to a directory (Polars expects files).
            Exception: Propagates errors from polars.read_excel (e.g., sheet not found).
        """
        # Combine sheet_name logic with kwargs for read_excel
        read_kwargs = kwargs.copy()
        if sheet_name is not None:
            read_kwargs['sheet_name'] = sheet_name
        # else: read_excel defaults to first sheet

        # self.logger.info(f"Parsing XLSX source: {self.source} (Sheet: {sheet_name or 'First'}) with options: {kwargs}")

        # Basic check for single path source
        if isinstance(self.source, (str, Path)):
            source_path = Path(self.source)
            if source_path.is_dir():
                msg = f"Source path cannot be a directory for XlsxParser: {self.source}"
                self.logger.error(msg)
                raise IsADirectoryError(msg)
            # FileNotFoundError will be caught by the decorator if it doesn't exist
        # If self.source is a list, assume they are valid file paths.
        # Polars might raise error if any list item is a directory or doesn't exist.

        df = pl.read_excel(source=self.source, **read_kwargs)
        return df.lazy()