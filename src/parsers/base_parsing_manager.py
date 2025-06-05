import logging
from pathlib import Path
from abc import ABC, abstractmethod
import inspect
import importlib

# TODO: Add support for codebook and config parsers
from src.parsers.domain_data_parsers.base_parse_domain import BaseDomainDataParser

# from codebook_parsers.base_parse_codebook import BaseCodebookParser
# from config_parsers.base_parse_config import BaseConfigParser


class BaseParsingManager(ABC):
    """Abstract class for parsing managers that discover and load parser modules
    dynamically (specific to parser type, eg. BaseDomainParser)."""

    # self.default_args: dict[str, dict[str, any]] = {}

    def __init__(
        self,
        logger: logging.Logger,
        white_list: list[str],
        parsers: Path,
        input_paths: Path
    ):
        # 1. Set core attributes
        self.logger = logger
        self.white_list = white_list
        self.parsers = parsers
        self.input_paths = input_paths


        self.parser_types = [BaseDomainDataParser]

        self.validated_parsers: dict[str, any] = {}
        self.data_sources: list[Path] = []

        # self.validated_data_sources: list[Path] = []
        # self.data_to_parser_map: dict[Path, Path] = {}

        # 4. Populate the first two
        #self._find_data()
        #self._find_parsers()

    # -------------------------------------------------------------------------
    

    # u don tneed the other function. rename this
    # path for path, str for module paths
    def _find_data(self) -> None:
        """
        Returns all files from the directory, supporting both filesystem paths and
        dot-separated module paths (e.g., 'src.parsers.domain_data_parsers').
        """
        # If already a Path or a string path that exists
        if isinstance(self.input_paths, Path):
            path = Path(self.input_paths)

            # esist check
            if not path.is_dir():
                raise NotADirectoryError(f"{self.input_paths} is not a directory.")
            files = [file for file in path.iterdir() if file.is_file()]
            if not files:
                raise ValueError(f"Directory is empty: {path}")
            self.data_sources = files

        # If a string that is a module path, try to resolve to filesystem path
        if isinstance(self.input_paths, str):
            spec = importlib.util.find_spec(self.input_paths)
            if spec is None or not spec.submodule_search_locations:
                raise ValueError(f"Could not resolve module path: {self.input_paths}")
            module_dir = Path(next(iter(spec.submodule_search_locations)))
            files = [file for file in module_dir.iterdir() if file.is_file()]
            if not files:
                raise ValueError(f"Module directory is empty: {module_dir}")
                
            self.data_sources = files

        raise TypeError("directory_path must be a Path or a string (filesystem or module path).")


    def _find_parsers(self) -> None:
        """Discovers and validates parsers in the specified directory"""
        
        # TODO it should work for config and codebook parsers too
        # Find all parser files in directory
        for py_file in self._get_files_from_directory(self.parsers):
            if py_file.suffix == ".py" and py_file.stem.startswith("parse_"):
                
                try:
                    module_name = py_file.stem
                    module = importlib.import_module(f"{self.parsers}.{module_name}")

                    for name, cls in inspect.getmembers(module, inspect.isclass):
                        # Check if cls is a subclass of any allowed parser type (but not the base itself)
                        is_valid_parser = False
                        for base in self.parser_types:
                            if issubclass(cls, base) and cls is not base:
                                is_valid_parser = True
                                break
                        if not is_valid_parser:
                            continue
                    
                    if not hasattr(cls, "SUPPORTED_TYPE"):
                        raise ValueError(
                            f"{name} is missing the required SUPPORTED_TYPE attribute."
                        )
                        
                    supported_type = getattr(cls, "SUPPORTED_TYPE")
                    if not isinstance(supported_type, str) or not supported_type.strip():
                        raise ValueError(
                            f"{name} has an invalid SUPPORTED_TYPE attribute: {supported_type}"
                        )
                    
                    if not hasattr(cls, "parse_file") or not callable(cls.parse_file):
                        raise ValueError(
                            f"{name} is missing the required parse_file() method."
                        )
                    
                    # This would prevent one from having multiple parsers for
                    # the same file type, which could be necessary in the future
                    # for now it's fine.
                    file_type = supported_type.lower()
                    if file_type in self.validated_parsers:
                        raise ValueError(
                            f"Duplicate parser for '{file_type}'. "
                            f"Existing: {self.validated_parsers[file_type].__name__}, "
                            f"New: {name}"
                        )
                    else:
                        self.validated_parsers[file_type] = cls
                        self.logger.info(
                            f"Registered parser: {name} for type '{file_type}'"
                        )

                    if not self.validated_parsers:
                        raise ValueError("No valid parsers found.")
                        
                except ImportError as e:
                    self.logger.error(f"Failed to import {py_file}: {str(e)}")
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error processing {py_file}: {str(e)}",
                        exc_info=True
                    )
                    raise

        return self.validated_parsers

    # -------------------------------------------------------------------------

    #@abstractmethod
    def _map_data_to_parsers(self) -> None:
        pass

    #@abstractmethod
    def parse_data(self): #-> pl.LazyFrame | None:
        pass

    def get_parser_for_type(self, file_type: str):
        """Get the appropriate parser for the given file type."""
        file_type = file_type.lower()
        if file_type not in self.validated_parsers:
            raise KeyError(f"No parser found for file type: {file_type}")
        return self.validated_parsers[file_type]

    # ------------------------------
    # @abstractmethod
    def _update_default_args(self):
        # placeholder for user inputs to default args
        pass
