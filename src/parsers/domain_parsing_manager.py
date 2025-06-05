import logging
import json
import hashlib
import csv

from pathlib import Path
#from typing import override python 12
import polars as pl

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType

#from domain_data_parsers.base_parse_domain import BaseDomainDataParser
from src.parsers.base_parsing_manager import BaseParsingManager


class DomainParsingManager(BaseParsingManager):
    def __init__(
        self, 
        logger: logging.Logger, 
        white_list: list[str],
        parsers: Path,
        input_paths: Path,
        filtered_dd_mirror: Path,

        # maybe even one level higher - abc
        parsing_options: dict[str, bool],
    ):
        super().__init__(
            logger,
            white_list,
            parsers,
            input_paths
        )

        self.filtered_dd_mirror = filtered_dd_mirror
        self.ingestion_tracker_path = filtered_dd_mirror / "ingestion_tracker.json"
        self.catalog_db_path = filtered_dd_mirror / "pyiceberg_catalog.db"
        self.add_id = parsing_options["add_id"]
        self.source_csv_structure_analysis_path = filtered_dd_mirror / "structure_analysis.json"
        self.default_args = {
            "csv": {
                "infer_schema_length": 0
            },
            "xlsx": {
                "infer_schema_length": 0
            },
            "parquet": {},
            "sqlite": {}
        }

    # ----------------------------------------------------------------
        
    def parse_all(self) -> pl.LazyFrame:
        """Parse all data sources in the input directory to iceberg tables.

        The ingestion_tracker.json file is used to keep track of which files have
        already been parsed (and their checksums at time of parsing).

        Hardoop cataloge is used to store the parsed data.

        Args:
            None

        Returns:
            A single Polars LazyFrame containing all the parsed data.
        """

        self.logger.info("\n\n --- PARSING DOMAIN DATA ---")


        catalog = load_catalog(
            "default",
            **{
                'type': 'sql',
                "uri": f"sqlite:///{self.catalog_db_path}",
                "warehouse": f"file://{self.filtered_dd_mirror}",
            },
        )

        if self.ingestion_tracker_path.exists():
          with open(self.ingestion_tracker_path, "r") as f:
            ingestion_tracker = json.load(f)

            table = catalog.load_table("default.domain_data")
        else:
            ingestion_tracker = {}
            fields = []
            column_id = 1
            for col in self.white_list:
                # Add original column as StringType, optional
                fields.append(NestedField(column_id, name=col, field_type=StringType(), required=False))
                column_id += 1
                # Add _processed column as StringType, optional
                #fields.append(NestedField(id, name=f"{col}_original", field_type=StringType(), required=False))
                #id += 1

            fields.append(NestedField(column_id, name="file_name", field_type=StringType(), required=False))
            column_id += 1

            if self.add_id:
                fields.append(NestedField(column_id, name="pyCura_id", field_type=StringType(), required=False))
                column_id += 1

            catalog.create_namespace("default")
            table = catalog.create_table(
                "default.domain_data",
                schema=Schema(*fields),
                location=str(self.filtered_dd_mirror / "default" / "domain_data"),
            )
        
        # ---------------------------------------------------------
        def _lookup_file(file_path: Path, ingestion_tracker: dict):
            """Parse or reuse Parquet for a single data source."""

            # Move to utils
            def _compute_checksum(file_path: Path) -> str:
                """Compute SHA256 checksum of the source file."""
                hasher = hashlib.sha256()
                with open(file_path, "rb") as f_obj:
                    for chunk in iter(lambda: f_obj.read(8192), b""):
                        hasher.update(chunk)
                return hasher.hexdigest()

            
            parse = False
            checksum = _compute_checksum(file_path)
            
            if file_path.name in ingestion_tracker:
                if ingestion_tracker[file_path.name]["checksum"] == checksum:
                    self.logger.info(f"File {file_path.name} already parsed, skipping.")
                    return checksum, parse
                else:
                    self.logger.warning(f"File {file_path.name} has changed. Skipping.")
                    self.logger.warning("Revert changes made, or reparse entire domain.*")
                    self.logger.warning("(*This will rewrite the entire domain mirror)")

                    return checksum, parse
            else:
                parse = True

                self.logger.info(f"File {file_path.name} not found in ingestion tracker, parsing.")

            return checksum, parse
        # ---------------------------------------------------------


        #TODO: Does not account for files that are appended
        is_valid = True
        if self.ingestion_tracker_path.exists():
            # print schema
            
            skip = input("Ingestion tracker exists. Press 'y' and Enter to run checksum check, or Enter to skip: ")
            if skip.lower() != "y":
                self.logger.info("Skipping checksum check.")

                parsed_table = pl.scan_iceberg(table)
                
                return parsed_table

        is_valid = self._inspect_csv_structure()

        if not is_valid:
            raise ValueError("Input CSV structure is invalid. See structure_analysis.json for details.")
        
        total_files = len(list(self.input_paths.iterdir()))
        current_id = 1

        for file_path in self.input_paths.iterdir():
            if file_path.is_file():

                checksum, parse = _lookup_file(file_path, ingestion_tracker)

                if parse: # ----------- DEBUG ! -----------
                    
                    #CONFIG SPEC ! SEPARATOR IS ;

                    separator = self.structure_analysis["file_separators"][file_path.name]
                    
                    lf =  pl.scan_csv(file_path, infer_schema_length=0, separator=separator)
                    #lf =  pl.scan_csv(file_path, infer_schema_length=0, separator=",") # get the namespace error
                    
                    # check if all whitelist columns are present in lf
                    #print(lf.collect_schema().names())
                    
                    missing_cols = [c for c in self.white_list if c not in lf.collect_schema().names()]
                    if missing_cols:
                        raise ValueError(f"File {file_path.name} is missing columns: {missing_cols}")

                    cols = self.white_list
                    lf = lf.select(cols)

                    # HERE WE ADD FILE_NAME COLUMN !
                    lf = lf.with_columns(pl.lit(file_path.name).alias("file_name"))

                    # HERE WE ADD ID COLUMN !
                    # ID might need to reset per file
                    if self.add_id:
                        # Collect row count efficiently
                        n_rows = lf.select(pl.count()).collect().item()
                        # Add a unique, incrementing id per row, id is string
                        lf = lf.with_columns(pl.arange(current_id, current_id + n_rows).alias("pyCura_id").cast(pl.Utf8))
                        # make sure it is a string

                        current_id += n_rows

                    ## This will not add to the global whitelist
                    #self.white_list.insert(0, "id")

                    # HERE WE APPEND TO TABLE !
                    table.append(lf.collect(engine='streaming').to_arrow())
                    
                    
                    #for snapshot in table.metadata.snapshots:
                     #   print(str(snapshot))

                    ingestion_tracker[file_path.name] = {
                        "checksum": checksum,
                        "snapshot": str(table.metadata.snapshots[-1])
                    }

                    with open(self.ingestion_tracker_path, "w") as f:
                        json.dump(ingestion_tracker, f, indent=4)
                    
                    total_files -= 1
                    self.logger.info(f"Parsed {file_path.name} - {total_files} files remaining")

        parsed_table = pl.scan_iceberg(table)

        return parsed_table


    def _inspect_csv_structure(self) -> bool:
        """
        Validate CSV files structure and identify common columns across all files.
        
        Returns:
            tuple containing:
                - List of columns common to all CSV files
                - Boolean indicating if all whitelist columns are present in common columns
        
        Also exports a JSON file with column structure analysis.
        """
        try:
            if not any(self.input_paths.iterdir()):
                raise FileNotFoundError(f"No CSV files found in {self.input_paths}")
        except Exception as e:
            self.logger.error(str(e))
            raise
        
        # Dictionary to store headers for each file
        all_headers = {}
        # Set to track common columns (will be intersected as we process files)
        common_columns = set()

        # Dictionary to store detected separators for each file
        detected_separators = {}
        
        # Function to detect the most likely separator in a CSV file
        def detect_separator(file_path):

            # TODO: DOUBLE CHECK THIS !!!
            potential_separators = [',', ';', '\t', '|']
            separator_counts = {sep: 0 for sep in potential_separators}
            

            # TODO: handle encoding errors , errors='ignore'
            # Read a sample of the file (first few lines)
            with open(file_path, 'r', encoding='utf-8') as f:
                sample_lines = [f.readline() for _ in range(min(5, sum(1 for _ in open(file_path))))]
            
            # Count occurrences of each potential separator in each line
            for line in sample_lines:
                for sep in potential_separators:
                    # If separator appears consistently in each line, increment its count
                    if line.count(sep) > 0 and line.count(sep) == sample_lines[0].count(sep):
                        separator_counts[sep] += 1
            
            # Find the separator with the highest consistent count
            most_likely_separator = max(separator_counts.items(), key=lambda x: x[1])[0]
            
            # If no clear separator is found, default to comma
            if separator_counts[most_likely_separator] == 0:
                return ','
            
            return most_likely_separator
        
        # You could do 2in1 - detect and header - but we'll leave it like this for now

        # Detect separators for each CSV file
        for csv_file in self.input_paths.iterdir():
            if not csv_file.is_file():
                self.logger.warning(f" -> SKIPPING {csv_file.name} - Not a file")
                continue
            separator = detect_separator(csv_file)
            detected_separators[csv_file.name] = separator
            self.logger.info(f" -> DETECTED SEPARATOR '{separator}' FOR FILE {csv_file.name}")
        
        
        # TODO: handle encoding errors
        # Process each CSV file - , errors='replace'
        for idx, csv_file in enumerate(self.input_paths.iterdir()):
            if not csv_file.is_file():
                self.logger.warning(f" -> SKIPPING {csv_file.name} - Not a file")
                continue
            
            with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=detected_separators[csv_file.name])
                headers = next(reader)
                all_headers[csv_file.name] = headers
                
                # For first file, initialize common_columns
                if idx == 0:
                    common_columns = set(headers)
                else:
                    # Intersect with current file's headers to find common columns
                    common_columns = common_columns.intersection(set(headers))
        
        # Convert common_columns from set back to list for consistent ordering
        common_columns_list = sorted(list(common_columns))
        
        # Determine special columns in each file
        file_special_columns = {}
        for filename, headers in all_headers.items():
            special_columns = [col for col in headers if col not in common_columns]
            if special_columns:
                file_special_columns[filename] = special_columns
        
        # Check if whitelist columns are in common columns
        missing_whitelist_columns = [col for col in self.white_list if col not in common_columns]
        is_valid = len(missing_whitelist_columns) == 0
        
        # Prepare results for JSON export
        self.structure_analysis = {
            "file_separators": detected_separators,
            "common_columns": common_columns_list,
            "file_special_columns": file_special_columns,
            "whitelist_columns": self.white_list,
            "missing_whitelist_columns": missing_whitelist_columns,
            "is_valid": is_valid
        }
        
        # Export to JSON
        with open(self.source_csv_structure_analysis_path, "w") as f:
            json.dump(self.structure_analysis, f, indent=4)

        if missing_whitelist_columns == self.white_list:
            #something went wrong
            self.logger.error(" -> ALL REQUIRED WHITELIST COLUMNS ARE MISSING FROM ALL CSV FILES.") 
            self.logger.error(f" -> See {self.source_csv_structure_analysis_path} for details")
            raise ValueError(" -> ALL REQUIRED WHITELIST COLUMNS ARE MISSING FROM ALL CSV FILES")

        # Log results
        if not file_special_columns:
            self.logger.info(" -> ALL CSV CONTAINT THE SAME COLUMNS")
        else:
            self.logger.warning(" -> CSV FILES HAVE INCONSISTENT STRUCTURE.")  
            self.logger.warning(f" -> See {self.source_csv_structure_analysis_path} for details\n")

        if not missing_whitelist_columns: # == VALID
            self.logger.info(" -> ALL REQUIRED WHITELIST COLUMNS ARE PRESENT ACROSS CSV FILES - THE STRUCTURE IS VALID")
        else:
            self.logger.warning(f"\n -> MISSING WHITELIST COLUMNS: {missing_whitelist_columns}")

        return self.structure_analysis

    # -------------------- <> for now --------------------
    # @override
    def parse_data(
        self,
        source: Path,
        file_type: str,
        **kwargs) -> pl.LazyFrame:

        if not isinstance(file_type, str):
            raise TypeError("file_type must be a string.")

        parser_class = self.get_parser(file_type)
        if not parser_class:
            self.logger.error(f"No parser found for file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        self.logger.info(
            f"Using parser {parser_class.__name__} for file type '{file_type}' and source '{source}'"
        )

        # The parse method itself now handles errors via the decorator in baseclass
        parser_instance = parser_class(source, logger=self.logger)
        
        return parser_instance.parse(**kwargs)

    #@override
    def _validate_data(self, data_path: Path) -> None:
        # ensure path exists and is a file
        if not data_path.exists():
            raise FileNotFoundError(f"Data file not found: {data_path}")
        if not data_path.is_file():
            raise TypeError(f"Not a file: {data_path}")
        # check for supported file type
        file_type = data_path.suffix.lstrip('.').lower()
        if file_type not in self.validated_parsers:
            raise ValueError(f"Unsupported file type: {file_type}")
        # quick schema sniff for CSV/Parquet
        try:
            if file_type == 'csv':
                pl.scan_csv(data_path, n_rows=1)
            elif file_type == 'parquet':
                pl.scan_parquet(data_path, n_rows=1)
        except Exception as e:
            raise ValueError(f"Failed to validate {file_type} file {data_path}: {e}")
