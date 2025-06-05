import importlib
from src.shared.utils import export_to_json
from src.shared.utils import merge_dicts
import shutil
import time
import json

import polars as pl

from src.parsers.domain_parsing_manager import DomainParsingManager


# These should be moved upstream
class DomainProcessingError(Exception):
    """Base exception for domain data processing errors."""
    pass


class ParsingError(DomainProcessingError):
    """Exception raised when there's an error parsing domain data."""
    pass


class InspectionError(DomainProcessingError):
    """Exception raised when there's an error during domain data inspection."""
    pass


class EditError(DomainProcessingError):
    """Exception raised when there's an error editing domain data."""
    pass


class ExportError(DomainProcessingError):
    """Exception raised when there's an error exporting domain data."""
    pass


class DomainDataProcessor:
    def __init__(self, dd_injection):
        """Initialize DomainDataProcessor with a ConfigHandler instance."""
        self.logger = dd_injection["logger"]
        self.white_list = dd_injection["whitelist"]

        self.module_paths = dd_injection["module_paths"]
        self.dd_parsers = dd_injection["module_paths"]["dd_parsers"]

        self.domain_input_paths = dd_injection["input_paths"]["domain"]
        self.filtered_dd_mirror = dd_injection["buffer_paths"]["filtered_dd_mirror"]

        self.dd_inspections = dd_injection["dd_inspections"]

        #there two have inconsistent naming
        self.domain_exports = dd_injection["output_paths"]["domain_exports"]
        self.output_paths_dd = dd_injection["output_paths"]

        self.final_dd = dd_injection["output_paths"]["final_dd"]
        
        self.parsing_options = dd_injection['parsing_options']

        self.csv_export_delimiter = dd_injection.get("csv_export_delimiter", ",")
        
        # Default output format and batching if not specified in config
        self.output_formats_and_batching = dd_injection.get(
            "output_formats_and_batching", {"csv": "mirror_input"}
        )

        self.to_select = []
        self.parsed_table = None
        
        # 
        self.parsing_manager = DomainParsingManager(
            self.logger,
            self.white_list,
            self.dd_parsers,
            self.domain_input_paths,
            self.filtered_dd_mirror,
            self.parsing_options,
        )

        # validated parsers
        # data_sources / validated data sources
        # data_to_parser_map

    def _print_table_metadata(self):
        """Print metadata about the parsed table for debugging purposes."""
        try:
            self.logger.info(self.parsed_table.collect_schema())
            self.logger.info("\n")
            n_rows = self.parsed_table.select(pl.count()).collect().item()
            self.logger.info(f"Total Rows: {n_rows}")
            with pl.Config(tbl_cols=20, tbl_rows=20):  # Show up to 20 columns in output
                print(self.parsed_table.head(10).collect())
        except Exception as e:
            self.logger.error(f"Error printing table metadata: {e}")

    def run_domain_pre_processing(self):
        """Parse domain data and prepare it for further processing."""
        self.logger.info("\n\n --- PARSING DOMAIN DATA ---")
        
        try:
            # Parse all data using the parsing manager
            self.parsed_table = self.parsing_manager.parse_all()

            # Log schema and sample data
            self.logger.info("\n\n (Whitelisted) Buffer Schema (DATA_BUFFER) && Sample Data: \n")
            self._print_table_metadata()
            
            self.logger.info(" -> DOMAIN DATA EXPORTED TO PROJECT BUFFER FOLDER")
            
        except Exception as e:
            self.logger.error(f"Error during domain pre-processing: {str(e)}")
            
            # Only attempt cleanup if the mirror directory exists
            if self.filtered_dd_mirror.exists():
                self.logger.warning(f" -> ERROR OCCURRED. INPUT MIRROR (BUFFER) FOLDER MAY NEED CLEANUP: {self.filtered_dd_mirror}")
                
                # Ask for confirmation before removing the directory
                confirmation = input(
                    "\n > Error while parsing. Press y to remove domain mirror folder, or press Enter to continue: ")
                if confirmation.lower() == "y":
                    try:
                        shutil.rmtree(self.filtered_dd_mirror)
                        self.logger.info(f" -> REMOVED MIRROR FOLDER: {self.filtered_dd_mirror}")
                    except Exception as cleanup_error:
                        self.logger.error(f"Error removing mirror folder: {cleanup_error}")
            
            # Re-raise as a ParsingError with the original exception as context
            raise ParsingError(f"Failed to parse domain data: {str(e)}") from e
            
        self.logger.info(" --- DOMAIN PRE-PROCESSING COMPLETE ---")

    def run_inspection_processing(self, second_run: bool):
        """Run inspection functions on the parsed data."""
        self.logger.info("\n\n --- RUNNING DOMAIN INSPECTION ---")
        
        if self.parsed_table is None:
            raise InspectionError("Cannot run inspection: No parsed data available")
            
        # Get the inspections to run from the config file
        inspections_to_run = self.dd_inspections

        # Inspection_name (key) is the name of the inspection function
        # The config (value) is the config for the inspection -> better varname
        for inspection_name, config in inspections_to_run.items():
            if not config["active"]:
                continue

            try:
                # ---- TAGS ----
                subkey_tag = inspection_name
                if second_run:
                    subkey_tag += "_PROCESSED"

                inspection_tag = "DOMAIN_DATA"
                if "-values" in inspection_name:
                    raise ValueError("-values is not supported for domain data inspection")
                
                inspection_tag += "_" + inspection_name

                # ---- PREPARE EXPORT JSON ----
                target_values = False
                inspection_export = {key: {} for key in self.white_list}

                if not second_run:
                    setattr(self, inspection_tag, inspection_export)

                # ---- IMPORTING INSPECTION FUNCTION ----
                try:
                    inspection_module = importlib.import_module(
                        f"{self.module_paths['inspections']}.{inspection_name}"
                    ) # why not in class namespace
                    inspection_function = getattr(inspection_module, f"{inspection_name}")
                except (ImportError, AttributeError) as e:
                    raise InspectionError(f"Failed to import inspection function '{inspection_name}': {str(e)}")
                
                # ---- GETTING METADATA ----
                schema = self.parsed_table.collect_schema()
                n_cols = len(schema.names())
                
                try:
                    n_rows = self.parsed_table.select(pl.count()).collect().item()
                except Exception as e:
                    self.logger.warning("Error getting row count - setting to '?'")
                    self.logger.error(str(e))
                    n_rows = "?"
                
                # ---- RUNNING INSPECTION ----
                self.logger.info(f"Running inspection: {inspection_name} on {n_rows} rows and {n_cols} columns")
                
                inspection_result = inspection_function(
                    (self.parsed_table, self.white_list), target_values
                )

                # Merge results with existing data
                merged = merge_dicts(
                    getattr(self, inspection_tag), inspection_result, subkey_tag
                )

                setattr(self, inspection_tag, merged)

                # Export results to JSON
                export_to_json(
                    getattr(self, inspection_tag),
                    self.output_paths_dd["inspection"],
                    inspection_tag,
                )
                
                self.logger.info(
                    f" -> EXPORTED {inspection_tag} TO {self.output_paths_dd['inspection']}"
                )
                
            except Exception as e:
                self.logger.error(f"Error running inspection '{inspection_name}': {str(e)}")
                # Continue with next inspection rather than failing the entire process
                continue
    
    def run_edit(self, key, edit, parameters):
        """Apply an edit function to the parsed data."""
        if self.parsed_table is None:
            raise EditError("Cannot run edit: No parsed data available")
            
        self.logger.info(f"Running edit: {edit} on column '{key}' with parameters: {parameters}")
        
        try:
            # Import the edit module
            try:
                edit_module = importlib.import_module(
                    f"{self.module_paths['edits']}.{edit}"
                )
                edit_function = getattr(edit_module, f"{edit}")
            except (ImportError, AttributeError) as e:
                raise EditError(f"Failed to import edit function '{edit}': {str(e)}")

            # Validate parameters
            if not isinstance(parameters, (list, tuple)):
                raise EditError(f"Parameters must be a list or tuple, got {type(parameters)}")

            # Apply the edit function to the parsed table
            original_schema = self.parsed_table.collect_schema()
            
            # Apply edit and capture result
            result = edit_function((self.parsed_table, key), *parameters)
            
            # Validate result is a LazyFrame
            if not isinstance(result, pl.LazyFrame):
                raise EditError(f"Edit function '{edit}' did not return a LazyFrame")
                
            self.parsed_table = result
            
            # Update whitelist if a new column was added
            if edit == "append_column" and key not in self.white_list:
                self.white_list.append(key)
                self.logger.info(f" -> ADDED NEW FIELD '{key}' TO WHITE LIST")
                
            # Log schema changes
            new_schema = self.parsed_table.collect_schema()
            added_columns = set(new_schema.names()) - set(original_schema.names())
            removed_columns = set(original_schema.names()) - set(new_schema.names())
            
            if added_columns:
                self.logger.info(f" -> ADDED COLUMNS: {added_columns}")
            if removed_columns:
                self.logger.info(f" -> REMOVED COLUMNS: {removed_columns}")
                
        except Exception as e:
            self.logger.error(f"Error applying edit '{edit}' to column '{key}': {str(e)}")
            raise EditError(f"Failed to apply edit '{edit}' to column '{key}': {str(e)}") from e
            
    def print_edited_table_sample(self):
        """Print a sample of the edited table."""
        if self.parsed_table is None:
            self.logger.warning("No parsed table available to display")
            return
            
        self.logger.info("\n\n Edited Schema && (Final State) Sample Data: \n")
        self.logger.info("Note: pyCura_id and file_name are only exported if specified in the config file.")
        self._print_table_metadata()

    def run_export(self):
        """Export the processed domain data."""
        if self.parsed_table is None:
            raise ExportError("Cannot export: No parsed data available")
            
        try:
            self._export_domain_data()
        except Exception as e:
            self.logger.error(f"Error during export: {str(e)}")
            raise ExportError(f"Failed to export domain data: {str(e)}") from e

    # -----------------------------------------------------------------------------------------------------
    def _export_domain_data(self):
        """
        Export domain data in specified formats with specified batching strategies.
        
        Supports multiple formats:
        - csv: Standard CSV format
        - parquet: Apache Parquet columnar format
        - feather: Apache Arrow Feather format
        
        Supports multiple batching strategies:
        - monolith: All data in a single file
        - mirror_input: One output file per input file
        - numeric value (e.g., "100000"): Partition by row count
        """
        try:
            # Load ingestion tracker
            tracker_path = self.filtered_dd_mirror / "ingestion_tracker.json"
            if not tracker_path.exists():
                raise ExportError(f"Ingestion tracker not found at {tracker_path}")
                
            with open(tracker_path, "r") as f:
                ingestion_tracker = json.load(f)
        
            self.logger.info("\n\n --- EXPORTING DOMAIN DATA ---")

            # Prepare columns to select
            self.to_select = []
            if self.parsing_options.get("add_id", False):
                self.to_select.append("pyCura_id")
            # add_file_name
            # We could also insert at a specific position
            self.to_select += self.white_list

            # DEBUG
            #self.logger.info(self.lazy_df.explain(streaming=True))

            # Process each format and batching strategy
            for format_name, batching in self.output_formats_and_batching.items():
                start = time.time()
                self.logger.info(f"\n --- EXPORTING AS {format_name.upper()} WITH {batching} BATCHING ---")
                
                # Create format-specific directory
                format_dir = self.domain_exports / format_name
                format_dir.mkdir(exist_ok=True, parents=True)
                
                # Call the appropriate format-specific export function
                # Could be csv, parquet, feather
                export_method = getattr(self, f"_export_{format_name.lower()}", None)
                if export_method is None:
                    self.logger.warning(f"Unknown format: {format_name}. Skipping export.")
                    continue
                    
                export_method(format_dir, batching, ingestion_tracker)

                end = time.time()
                self.logger.info(f"Exporting as {format_name.upper()} with {batching} batching took {end - start:.2f} seconds")
                
            self.logger.info(" -> DOMAIN DATA EXPORTED TO DATA_OUT FOLDER")
            
        except Exception as e:
            self.logger.error(f"Error in _export_domain_data: {str(e)}")
            raise ExportError(f"Failed to export domain data: {str(e)}") from e

        

    def _export_csv(self, output_dir, batching, ingestion_tracker):
        """
        Export data to CSV format with the specified batching strategy.
        
        Args:
            output_dir: Directory to write CSV files to
            batching: batching strategy (monolith, mirror_input, or numeric value)
            ingestion_tracker: Dictionary mapping input files to metadata
        """
        
        
        # For now, the monolith option extends mirror_input by concatenating all files
        # This is not the most efficient way, but polars >1.26.0 forces ...

        count = 1
        total = len(ingestion_tracker)
        separator = self.csv_export_delimiter

            
        if batching == "monolith":

            file_path = output_dir / "domain_data_monolith.csv"
            # ------------- Works with polars 1.30.0, but loads all data into memory
            #self.logger.info(f"Exporting to {file_path}... (no streming)")
            #df = lazy_df.collect()
            
            #df.write_csv(file_path, include_header=True, separator=',')
            #self.logger.info(f"Exporting to {file_path} using streaming...")
            # ------------------------------
           # self.lazy_df.sink_csv(
           #     file_path,
           #     include_header=True,
           #     #maintain_order=True,
           #     #batch_size=25
           #     engine='streaming'
           # )
            
            # collect each file from file_name given the ingestion tracker
            for file_name in ingestion_tracker:
                df = self.parsed_table.filter(pl.col("file_name") == file_name).select(self.to_select).collect()
                #write to csv
                df.write_csv(output_dir / file_name, include_header=True, separator=separator)
                self.logger.info(f"Exported {file_name}, {count}/{total}")
                count += 1
                
            #self.logger.info("CSV export completed using streaming")

            # Concatenate all files into one, use ingestion tracker again, but on the output ofc

            combined_df = None
            for file_name in ingestion_tracker:
                df = pl.read_csv(output_dir / file_name, infer_schema_length=0)
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = pl.concat([combined_df, df], how="vertical_relaxed")
            if combined_df is not None:
                combined_df.write_csv(file_path, include_header=True, separator=separator)
                self.logger.info("Concatenated all CSV files into domain_data.csv")


            #self.logger.info("CSV export completed using streaming")
                
        # Currently, the only way to proc Out-of-RAM - usong polars 1.26.0
        elif batching == "mirror_input":
            
            # collect each file from file_name given the ingestion tracker
            for file_name in ingestion_tracker:
                
                self.logger.info(f"Exporting to {file_name} using mirror_input...")
                df = self.parsed_table.filter(pl.col("file_name") == file_name).select(self.to_select).collect()
                
                #write to csv
                df.write_csv(output_dir / file_name, include_header=True, separator=separator)
                self.logger.info(f"Exported {file_name}, {count}/{total}")
                count += 1
                
            self.logger.info("CSV export completed using mirror_input")
        

        elif batching.isnumeric():
            import gc
                
            # Define a function to process a single batch
            def process_batch(lazy_frame, start_idx, batch_size, output_file):
                """Process a single batch in a separate function scope"""
                try:
                    # Get this batch
                    batch = lazy_frame.slice(start_idx, batch_size).collect()
                    
                    # Write to CSV
                    batch.write_csv(output_file, include_header=True, separator=separator)
                        
                    # Return batch size for logging
                    return len(batch)
                finally:
                    # Force cleanup
                    gc.collect()
                
            # Use the batch size to partition the data
            batch_size = int(batching)
            
            # Get row count (we need this for planning)
            n_rows = self.lazy_df.select(pl.count()).collect().item()
                
                # Process each batch in a separate function call
            for i in range(0, n_rows, batch_size):
                batch_file = output_dir / f"domain_data_batch_{i // batch_size + 1}.csv"
                
                # Process this batch
                rows_processed = process_batch(self.lazy_df, i, batch_size, batch_file)
                
                self.logger.info(f"Exported batch {i // batch_size + 1} ({i}–{i+rows_processed}) to {batch_file}")
                
                # Force garbage collection between batches
                gc.collect()
                
                self.logger.info("CSV export completed using batch partitioning")
            

    def _export_parquet(self, output_dir, batching, ingestion_tracker):
        pass
