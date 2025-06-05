import csv
import json
import importlib
from pathlib import Path

from src.shared.utils import filter_by_whitelist
from src.shared.utils import export_to_json
from src.shared.utils import merge_dicts


class CodebookProcessor:
    def __init__(self, cb_injection):
        """Initialize CodebookProcessor with a ConfigHandler instance."""
        self.logger = cb_injection["logger"]
        self.module_paths = cb_injection["module_paths"]
        self.white_list = cb_injection["whitelist"]

        self.input_paths = cb_injection["input_paths"]
        self.buffer_paths_cb = cb_injection["buffer_paths"]
        self.output_paths_cb = cb_injection["output_paths"]

        self.cb_inspections = cb_injection["cb_inspections"]
        self.key_export_ban = cb_injection["key_export_ban"]
        self.select_parser = cb_injection["select_parser"]
        self.append_new_metadata = cb_injection["append_new_metadata"]

    def _check_codebook_path(self) -> Path:
        # if we found that the filtered codebook mirror exists, we use it
        if self.buffer_paths_cb["f_filtered_cb_mirror"].exists():
            self.logger.info(" -> FOUND ALREADY PARSED CODEBOOK MIRROR JSON FILE.")
            self.logger.info(" -> WILL USE 'zero_parser' INSTEAD OF PARSING AGAIN.")
            return self.buffer_paths_cb["f_filtered_cb_mirror"]

        codebook_paths = []
        for file in self.input_paths["codebook"].iterdir():
            if file.is_file():
                # This will print all the codebook files in the codebook_input_path
                self.logger.info(f" -> CHECKING CODEBOOK FROM {file}")
                codebook_paths.append(file)

                # good fit?
        if len(codebook_paths) > 1:
            raise AssertionError(
                "More than one codebook file found in the codebook input path. \n"
                "Please check the codebook input path and remove any duplicate files. \n"
                "Note that the already parsed codebook json needs to be named \
                    'filtered_codebook_mirror.json'"
            )
        return codebook_paths[0]

    # TODO: At some point parser should be picked automatically.
    #       For now: use user input
    #       Later: remove and replace with automatic parser selection via manager
    def _parse_codebook(self) -> dict[str, dict[str, any]]:
        self.codebook_path = self._check_codebook_path()

        if self.codebook_path == self.buffer_paths_cb["f_filtered_cb_mirror"]:
            # zero_parsers basically just loads the codebook json
            parser_name = "zero_parser"
        else:
            parser_name = self.select_parser

        parser_module = importlib.import_module(
            f"{self.module_paths['cb_parsers']}.{parser_name}"
        )
        parser_function = getattr(parser_module, "parse_codebook")

        return parser_function(self.codebook_path, self.logger)

    # run_domain_processing
    def run_codebook_pre_processing(self):
        self.logger.info("\n\n --- PARSING CODEBOOK ---")
        try:
            self.codebook_path = self._check_codebook_path()
            self.parsed_codebook = self._parse_codebook()

            if not self.buffer_paths_cb["f_cb_mirror"].exists():
                with open(
                    self.buffer_paths_cb["f_cb_mirror"], "w", encoding="utf-8"
                ) as f:
                    json.dump(self.parsed_codebook, f, indent=4)

                    # why do we need this again?
            self.parsed_codebook = filter_by_whitelist(
                self.parsed_codebook, self.white_list
            )
            with open(
                self.buffer_paths_cb["f_filtered_cb_mirror"],
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(self.parsed_codebook, f, indent=4)

            self.logger.info(
                " -> CODEBOOK MIRROR(S) (BOTH UNFILTERED AND FILTERED) EXPORTED TO PROJECT BUFFER FOLDER"
            )
        except Exception:
            if self.buffer_paths_cb["f_cb_mirror"].exists():
                self.buffer_paths_cb["f_cb_mirror"].unlink()
            if self.buffer_paths_cb["f_filtered_cb_mirror"].exists():
                self.buffer_paths_cb["f_filtered_cb_mirror"].unlink()
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

            inspection_module = importlib.import_module(
                f"{self.module_paths['inspections']}.{inspection}"
            )
            inspection_function = getattr(inspection_module, f"{inspection}")
            inspection_result = inspection_function(
                self.parsed_codebook, target_values
            )

            merged = merge_dicts(
                getattr(self, inspection_tag), inspection_result, subkey_tag
            )

            setattr(self, inspection_tag, merged)

            export_to_json(
                getattr(self, inspection_tag),
                self.output_paths_cb["inspection"],
                inspection_tag,
            )
            self.logger.info(
                f" -> EXPORTED {inspection_tag} TO {self.output_paths_cb['inspection']}"
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

            # why did I write this function again?
            with open(
                self.output_paths_cb["f_final_cb"], "w", encoding="utf-8"
            ) as f:
                json.dump(self.parsed_codebook, f, indent=4)
            self.logger.info(
                f" -> EXPORTED final_codebook TO {self.output_paths_cb['f_final_cb']}"
            )

    def run_edit(self, key, edit, parameters) -> None:
        edit_module = importlib.import_module(
            f"{self.module_paths['edits']}.{edit}")
        edit_function = getattr(edit_module, f"{edit}")

        
        if edit != "append_column" and key != 'pyCura_id':
            data = edit_function(self.parsed_codebook["data"][key], *parameters)
            self.parsed_codebook["data"][key] = data
        else:
            # yet to be decided
            pass
        

    # TODO: Let the user decide output format
    #       For now we use csv
    def _export_keys_to_csv_files(self) -> None:
        """Export key-value pairs to CSV files without using pandas."""
        print(self.key_export_ban)
        for key, value in self.parsed_codebook["data"].items():
            if value and key not in self.key_export_ban:
                # Create CSV filename
                csv_path = self.output_paths_cb["key_exports"] / f"{key}.csv"
                # with open(csv_path, 'w', newline='', encoding='windows-1252') as f:
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
                empty_file_path = (
                    self.output_paths_cb["key_exports"]
                    / f"{key}_no_values.txt"
                )
                with open(empty_file_path, "w") as f:
                    f.write("")
                self.logger.info(f"Excluded {key}. Moving on.")

    def run_export(self):
        self._export_keys_to_csv_files()
