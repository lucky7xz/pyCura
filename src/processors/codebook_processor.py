import csv
import json
import importlib
from pathlib import Path
import logging
from typing import Dict, Any, List

from src.shared.utils import sort_whitelist, filter_by_whitelist, export_to_json, merge_dicts
from src.processors.base_processor import BaseProcessor
import shutil
import time


class CodebookProcessor(BaseProcessor):
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """Initialize CodebookProcessor with simple config and logger."""
        super().__init__(config, logger)
        
        # Codebook-specific config
        self.cb_inspections = config["cb_inspections"]
        self.key_export_ban = config["key_export_ban"]
        self.select_parser = config["select_parser"]
        self.append_new_metadata = config["append_new_metadata"]

    def _check_codebook_path(self) -> Path:
        # Use base class methods for path access
        filtered_cb_mirror = self.get_buffer_path("filtered_cb_mirror")
        
        if filtered_cb_mirror.exists():
            self.logger.info(" -> FOUND ALREADY PARSED CODEBOOK MIRROR JSON FILE.")
            self.logger.info(" -> WILL USE 'zero_parser' INSTEAD OF PARSING AGAIN.")
            return filtered_cb_mirror

        codebook_paths = []
        codebook_input = self.get_input_path("codebook")
        for file in codebook_input.iterdir():
            if file.is_file():
                # This will print all the codebook files in the codebook_input_path
                self.logger.info(f" -> CHECKING CODEBOOK FROM {file}")
                codebook_paths.append(file)

                # good fit?
        if len(codebook_paths) > 1:
            raise self.ConfigurationError(
                "More than one codebook file found in the codebook input path. \n"
                "Please check the codebook input path and remove any duplicate files. \n"
                "Note that the already parsed codebook json needs to be named \
                    'filtered_codebook_mirror.json'",
                file_path=str(codebook_input),
                data_context=f"Found {len(codebook_paths)} files: {[f.name for f in codebook_paths]}"
            )
        return codebook_paths[0]

    # TODO: At some point parser should be picked automatically.
    #       For now: use user input
    #       Later: remove and replace with automatic parser selection via manager
    def _parse_codebook(self) -> dict[str, dict[str, any]]:
        self.codebook_path = self._check_codebook_path()
        filtered_cb_mirror = self.get_buffer_path("filtered_cb_mirror")

        if self.codebook_path == filtered_cb_mirror:
            # zero_parsers basically just loads the codebook json
            parser_name = "zero_parser"
        else:
            parser_name = self.select_parser

        try:
            parser_module = importlib.import_module(
                f"src.parsers.codebook_parsers.{parser_name}"
            )
            parser_function = getattr(parser_module, "parse_codebook")
        except (ImportError, AttributeError) as e:
            raise self.ParsingError(
                f"Failed to load codebook parser '{parser_name}': {e}",
                file_path=str(self.codebook_path),
                data_context=f"Parser: {parser_name}"
            ) from e

        try:
            return parser_function(self.codebook_path, self.logger)
        except Exception as e:
            raise self.ParsingError(
                f"Codebook parsing failed with parser '{parser_name}': {e}",
                file_path=str(self.codebook_path),
                data_context=f"Parser: {parser_name}"
            ) from e

    def run_pre_processing(self):
        """Run codebook pre-processing."""
        self.logger.info("\n\n --- PARSING CODEBOOK ---")
        try:
            self.codebook_path = self._check_codebook_path()
            self.parsed_codebook = self._parse_codebook()

            # Use base class methods for path access
            cb_mirror = self.get_buffer_path("cb_mirror")
            filtered_cb_mirror = self.get_buffer_path("filtered_cb_mirror")

            if not cb_mirror.exists():
                with open(cb_mirror, "w", encoding="utf-8") as f:
                    json.dump(self.parsed_codebook, f, indent=4)

            self.parsed_codebook = filter_by_whitelist(
                self.parsed_codebook, self.white_list
            )
            with open(filtered_cb_mirror, "w", encoding="utf-8") as f:
                json.dump(self.parsed_codebook, f, indent=4)

            self.logger.info(
                " -> CODEBOOK MIRROR(S) (BOTH UNFILTERED AND FILTERED) EXPORTED TO PROJECT BUFFER FOLDER"
            )
        except Exception:
            cb_mirror = self.get_buffer_path("cb_mirror")
            filtered_cb_mirror = self.get_buffer_path("filtered_cb_mirror")
            if cb_mirror.exists():
                cb_mirror.unlink()
            if filtered_cb_mirror.exists():
                filtered_cb_mirror.unlink()
            raise
        self.logger.info(" --- CODEBOOK PRE-PROCESSING COMPLETE ---")

    def run_inspection_processing(self, second_run: bool):
        #if post_transformation:
        #    self.logger.info(
        #        " --- SKIPPING PARSING/FILTERING (POST-TRANSFORMATION MODE) ---"
        #    )
        #else:
        #    self.run_codebook_pre_processing()

        self.logger.info("\n\n --- RUNNING CODEBOOK INSPECTION ---")

        # get the inspections to run from the config file
        inspections_to_run = self.cb_inspections

        # inspection (key) is the name of the inspection function
        # the config (value) is the config for the inspection
        # change 'config' on next refac to something that is not confusing
        for inspection, config in inspections_to_run.items():
            if not config["active"]:
                continue

            target_values = False
            subkey_tag = inspection
            if second_run:
                subkey_tag += "_PROCESSED"

            inspection_tag = "CODEBOOK"
            if "-values" in inspection:
                inspection_tag += "_VALUES"
                inspection = inspection.replace("-values", "")
                target_values = True
            inspection_tag += "_" + inspection

            inspection_export = {}
            for key in self.white_list:
                inspection_export[key] = {}

            if not second_run:
                #if not hasattr(self, inspection_tag):
                setattr(self, inspection_tag, inspection_export)

            try:
                inspection_module = importlib.import_module(
                    f"src.processing_modules.inspections.{inspection}"
                )
                inspection_function = getattr(inspection_module, f"{inspection}")
            except (ImportError, AttributeError) as e:
                self.logger.error(f"Failed to load inspection '{inspection}': {e}")
                raise self.InspectionError(f"Failed to load inspection '{inspection}': {e}") from e
            inspection_result = inspection_function(
                self.parsed_codebook, target_values
            )

            merged = merge_dicts(
                getattr(self, inspection_tag), inspection_result, subkey_tag
            )

            setattr(self, inspection_tag, merged)

            inspection_output = self.get_output_path("inspection")
            export_to_json(
                getattr(self, inspection_tag),
                inspection_output,
                inspection_tag,
            )
            self.logger.info(
                f" -> EXPORTED {inspection_tag} TO {inspection_output}"
            )

            if self.append_new_metadata and second_run:
                # merge inspection results into codebook metadata
                self.parsed_codebook["metadata"] = merge_dicts(
                    self.parsed_codebook["metadata"],
                    inspection_result,
                    subkey_tag,
                )

        if second_run:
            # sort the values of the codebook
            for key, value in self.parsed_codebook["data"].items():
                value_sorted = dict(
                    sorted(value.items(), key=lambda item: item[0])
                )
                self.parsed_codebook["data"][key] = value_sorted

            # Export final codebook using base class method
            final_cb_path = self.get_output_path("final_cb")
            with open(final_cb_path, "w", encoding="utf-8") as f:
                json.dump(self.parsed_codebook, f, indent=4)
            self.logger.info(
                f" -> EXPORTED final_codebook TO {final_cb_path}"
            )

    def run_edit(self, key, edit, parameters) -> None:
        try:
            edit_module = importlib.import_module(
                f"src.processing_modules.edits.{edit}")
            edit_function = getattr(edit_module, f"{edit}")
        except (ImportError, AttributeError) as e:
            self.logger.error(f"Failed to load edit function '{edit}': {e}")
            raise self.EditError(f"Failed to load edit function '{edit}': {e}") from e

        try:
            if edit != "append_column" and key != 'pyCura_id':
                data = edit_function(self.parsed_codebook["data"][key], *parameters)
                self.parsed_codebook["data"][key] = data
            else:
                # yet to be decided
                pass
        except Exception as e:
            self.logger.error(f"Edit '{edit}' failed for key '{key}': {e}")
            raise self.EditError(f"Edit '{edit}' failed for key '{key}': {e}") from e
        

    # TODO: Let the user decide output format
    #       For now we use csv
    def _export_keys_to_csv_files(self) -> None:
        """Export key-value pairs to CSV files without using pandas."""
        print(self.key_export_ban)
        key_exports_path = self.get_output_path("key_exports")
        
        for key, value in self.parsed_codebook["data"].items():
            if value and key not in self.key_export_ban:
                # Create CSV filename
                csv_path = key_exports_path / f"{key}.csv"
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    # Write each key-value pair as a row
                    for k, v in value.items():
                        writer.writerow([k, v])
                self.logger.info(
                    f"Exported processed keys for {key} to {csv_path}"
                )
            else:
                # Create empty file for keys with no values
                empty_file_path = key_exports_path / f"{key}_no_values.txt"
                with open(empty_file_path, "w") as f:
                    f.write("")
                self.logger.info(f"Excluded {key}. Moving on.")

    def run_export(self):
        try:
            self._export_keys_to_csv_files()
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            raise self.ExportError(f"Export failed: {e}") from e
