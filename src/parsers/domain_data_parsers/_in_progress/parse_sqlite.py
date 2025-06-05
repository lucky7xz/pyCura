import polars as pl
from pathlib import Path
import logging
from src.parsers.domain_data_parsers.parsing_manager import BaseDomainDataParser, handle_parsing_errors

class SQLiteParser(BaseDomainDataParser):
    """Parses data from an SQLite database using a SQL query."""
    SUPPORTED_TYPE = 'sqlite'

    def __init__(self, source: str | Path, logger: logging.Logger | None = None):
        """
        Initializes the SQLite parser.

        Args:
            source: Path to the SQLite database file.
            logger: Logger instance.
        """
        # Perform basic validation on the source (should be a string path)
        if isinstance(source, (list, Path)):
            # Currently only support single DB file path as string
            # Could be extended to handle Path objects or check list length
            msg = "SQLiteParser source must be a string path to the database file."
            self.logger.error(msg)
            raise TypeError(msg)
        super().__init__(source, logger=logger)
        self.db_path = Path(self.source)
        # Further validation: check if file exists? depends on desired behavior
        # If Polars/connector handles non-existent file creation, maybe skip check.

    @handle_parsing_errors
    def parse(self, query: str, **kwargs) -> pl.LazyFrame:
        """
        Executes a SQL query against the SQLite database and returns a LazyFrame.
        Error handling is managed by the @handle_parsing_errors decorator.

        Args:
            query: The SQL query string to execute.
            **kwargs: Additional keyword arguments to pass directly to
                      polars.read_database (e.g., partition_on, batch_size).
                      Note: `connection` argument is derived from `self.source`.

        Returns:
            A Polars LazyFrame representing the query result.

        Raises:
            TypeError: If the provided query is not a string.
            Various database errors (e.g., OperationalError) from the connector
            if the query is invalid or the database connection fails.
        """
        if not isinstance(query, str):
            msg = f"Query must be a string, got: {type(query).__name__}"
            self.logger.error(msg)
            raise TypeError(msg)

        # Check file existence and type at parse time
        if self.db_path.is_dir():
             msg = f"SQLite source cannot be a directory: {self.db_path}"
             self.logger.error(msg)
             raise IsADirectoryError(msg)
        if not self.db_path.is_file():
             msg = f"SQLite database not found at: {self.db_path}"
             self.logger.error(msg)
             raise FileNotFoundError(msg)

        # Construct the connection URI if not provided in kwargs
        connection_uri = kwargs.pop('connection', kwargs.pop('connection_uri', None))
        if connection_uri is None:
             connection_uri = f"sqlite:///{self.db_path.resolve()}"

        # Use self.logger
        self.logger.info(f"Parsing SQLite source: {self.db_path} with query: '{query[:100]}{'...' if len(query) > 100 else ''}' using connection: {connection_uri} and options: {kwargs}")
        df = pl.read_database(query=query, connection=connection_uri, **kwargs)
        return df.lazy()