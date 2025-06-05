import json
import logging
from pathlib import Path

from src.shared.utils import sort_whitelist

# TODO: Add support for codebook and config parsers

#from parsers.config_parsers.base_parse_config import BaseConfigParser
#from parsers.codebook_parsers.base_parse_codebook import BaseCodebookParser
from src.parsers.domain_data_parsers.base_parse_domain import BaseDomainDataParser

#from processing_modules.edits.base_edit import BaseEdit
#from processing_modules.inspections.base_inspection import BaseInspection
#from processing_modules.checks.base_check import BaseCheck

class ProjectManager:
    """Base class for handling configuration and data paths across the pipeline."""

    def __init__(self, config_filename: str) -> None:
        """
        Initialize the ProjectManager. Specifies hardcoded paths, loads config file,
        sets up logging, creates necessary directories and can delete them.

        Args:
            config_filename: Name of the configuration file (e.g., 'config_0.json')
        """
        # -----------------SETUP LOGGING-------------------------
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # File handler (logs to file)
        file_handler = logging.FileHandler("cura_logs.txt")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(file_handler)
        self.log_file = Path("cura_logs.txt")

        # ----------------- LOAD CONFIG -------------------------
        config_folder = Path("config_files")
        self.config_path = config_folder / config_filename

        # _load_config check structure and values of config file
        self.config = self._load_config()

        # Used for DRY in inspection processing
        self.post_transformation = False

        self.logger.info("--------------  ----------------")
        self.logger.info(f" -> LOADED CONFIG: {self.config_path}")

        # -----------------DEFINE AND CHECK INPUT PATHS------------------------
        self.project_name = self.config.get("project_name", None)
        domain_foldername = self.config.get("domain_foldername", None)

        data_in_path = Path("data_in")
        data_buffer_path = Path("data_buffer")
        data_out_path = Path("data_out")

        # Project specific input paths
        domain_input_path = data_in_path / domain_foldername / "domain"
        codebook_input_path = data_in_path / domain_foldername / "codebook"

        self.logger.info(" -> CHECKING INPUT PATHS...")

        #Other config-file dependend paths/inputs are  
        if not domain_input_path.exists():
            raise AssertionError(f"input_path does not exist: {domain_input_path}")
        if not domain_input_path.iterdir():
            raise AssertionError(f"Input path(s) do not exist: {domain_input_path}")
        if not codebook_input_path.exists():
            raise AssertionError(f"input_path does not exist: {codebook_input_path}")
        if not codebook_input_path.iterdir():
            raise AssertionError(f"Input path(s) do not exist: {codebook_input_path}")

        self.input_paths = {
            "domain": domain_input_path,
            "codebook": codebook_input_path,
        }

        # -----------------DEFINE BUFFER PATHS-------------------------
        
        self.project_buffer_path = data_buffer_path / self.project_name

        self.buffer_paths_cb = {
            "f_cb_mirror": self.project_buffer_path / "original_cb_mirror.json",
            "f_filtered_cb_mirror": self.project_buffer_path / "filtered_cb_mirror.json",
        }

        self.buffer_paths_dd = {
            "filtered_dd_mirror": self.project_buffer_path / "buffer_dd"
        }
        self.mkdir_list = []
        self.mkdir_list.append(self.buffer_paths_dd["filtered_dd_mirror"])
        # -----------------DEFINE OUTPUT PATHS-------------------------
        self.project_export_path = data_out_path / self.project_name

        self.output_paths_cb = {
            "inspection": self.project_export_path / "inspection",
            "key_exports": self.project_export_path / "key_exports",
            "f_final_cb": self.project_export_path / "final_codebook.json",
        }

        self.output_paths_dd = {
            "inspection": self.project_export_path / "inspection",
            "domain_exports": self.project_export_path / "domain_exports",
            "final_dd": self.project_export_path / "final_data_data",
        }

        # 'parents=True' in _setup_directories allows to skip the rest 
        self.mkdir_list.append(self.output_paths_cb["inspection"])
        self.mkdir_list.append(self.output_paths_cb["key_exports"])
        self.mkdir_list.append(self.output_paths_dd["domain_exports"])
        self.mkdir_list.append(self.output_paths_dd["final_dd"])

        # --------------- DEFINE MODULE AND CHECK PATHS -------------------------
        # TODO - check if valid - see import statement
        self.module_paths = {
            #"checks": "src.processing_modules.checks",
            "edits": "src.processing_modules.edits",
            "inspections": "src.processing_modules.inspections",
            "dd_parsers": "src.parsers.domain_data_parsers",
            "cb_parsers": "src.parsers.codebook_parsers",
        }
        # --------------- SETUP INJECTIONS AND DIRECTORIES -------------------------

        self.cb_injection = {

            "logger": self.logger,
            "module_paths": self.module_paths,
            "whitelist": self.config["white_list"],
            
            "input_paths": self.input_paths,
            "buffer_paths": self.buffer_paths_cb,
            "output_paths": self.output_paths_cb,
            
            "cb_inspections": self.config["cb_inspections"],
            "key_export_ban": self.config["key_export_ban"],
            "select_parser": self.config["select_parser"],
            "append_new_metadata": self.config["append_new_metadata"]
        }

        self.dd_injection = {

            "logger": self.logger,
            "module_paths": self.module_paths,
            "whitelist": self.config["white_list"],
            "output_formats_and_batching": self.config["output_formats_and_batching"],
            "parsing_options": self.config["parsing_options"],
            
            "input_paths": self.input_paths,
            "buffer_paths": self.buffer_paths_dd,
            "output_paths": self.output_paths_dd,

            "dd_inspections": self.config["dd_inspections"],
            "csv_export_delimiter": self.config["csv_export_delimiter"],
            
        }

        self._setup_directories()
        self.logger.info(
            f" -> INITIALIZED PROJECT-MANAGER FOR '{self.project_name}'"
        )
        self.logger.info("--------------  ----------------")

    # --------------------------------------------------------------------------

    def _setup_directories(self) -> None:
        """Create necessary directory structure if it doesn't exist."""

        for directory in self.mkdir_list:
            directory.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------------
    def _validate_config(self, config: dict[str, any]) -> None:
        """Validate the configuration structure."""

        def check_key_content(key: str, key_type: type, config: dict[str, any]) -> None:
            value = config.get(key, None)

            if key_type is str:
                if not isinstance(value, str):
                    raise ValueError(f"{key} must be a string")
                if not value:
                    raise ValueError(f"{key} must not be empty string")
            elif key_type is list:
                if not isinstance(value, list):
                    raise ValueError(f"{key} must be a list")
                if not value:
                    raise ValueError(f"{key} must not be empty list")
            elif key_type is bool:
                if not isinstance(value, bool):
                    raise ValueError(f"{key} must be a boolean")
            elif key_type is dict:
                if not isinstance(value, dict):
                    raise ValueError(f"{key} must be a dictionary")
                if not value:
                    raise ValueError(f"{key} must not be empty dictionary")
            else:
                raise ValueError(f"{key} must be a {type}")

        # Check for required keys
        required_keys = {
            "project_name": str,
            "domain_foldername": str,
            "white_list": list,
            "append_new_metadata": bool,
            "select_parser": str,
            "cb_inspections": dict,
            "dd_inspections": dict,
            "edits": list,
            "csv_export_delimiter": str,
            "output_formats_and_batching": dict,
        }
        missing_keys = set(required_keys.keys()) - set(config.keys())

        if len(missing_keys) > 0:
            raise ValueError(
                f"Missing required configuration keys: {missing_keys}"
            )

        for key, key_type in required_keys.items():
            check_key_content(key, key_type, config)

    def _load_config(self) -> dict[str, any]:
        """Load and parse the configuration file."""
        if not self.config_path.exists():
            raise AssertionError(
                f"Configuration file not found at {self.config_path}. "
            )
        with open(self.config_path, "r", encoding="utf-8") as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError:
                raise AssertionError(
                    f"Invalid JSON in configuration file {self.config_path}"
                )

        if config.get("sub_configs", False):
            
            self.logger.info("Your selected config file has sub-configs. Please select one: \n")
            for i, config_name in enumerate(config["configs"].keys()):
                self.logger.info(f" Enter {i} for : {config_name}")
            
            user_input = input("\n> Enter the number of the sub-config: ")
            if not user_input.isdigit():
                raise ValueError("Invalid input - not a number")

            if not (0 <= int(user_input) < len(config["configs"])):
                raise ValueError("Invalid input - out of range")
            
            user_input = int(user_input)

            # use the sub-config to update the config - pj name, domain folder and append whitelist, each individually
            project_name = config["configs"][list(config["configs"].keys())[user_input]]["project_name"]
            domain_foldername = config["configs"][list(config["configs"].keys())[user_input]]["domain_foldername"]
            append_whitelist = config["configs"][list(config["configs"].keys())[user_input]]["white_list_append"]
            
            config["project_name"] = project_name
            config["domain_foldername"] = domain_foldername
            
            # append the append_whitelist to the whitelist
            config["white_list"] += append_whitelist

            # sort the whitelist
            config["white_list"] = sort_whitelist(config["white_list"])
            


        self._validate_config(config)
        return config

    # --------------------------------------------------------------------------

    def _delete_project_out_folder(self) -> None:
        """Delete the data_out folder to remove data from the previous pipeline run."""
        paths = sorted(
            self.project_export_path.rglob("*"),
            key=lambda x: len(x.parts),
            reverse=True,
        )

        for path in paths:
            if path.is_file():
                path.unlink()
            else:
                path.rmdir()

        self.project_export_path.rmdir()

    def _delete_project_buffer_folder(self) -> None:
        """Delete the data_buffer folder to remove data from the previous pipeline run."""
        paths = sorted(
            self.project_buffer_path.rglob("*"),
            key=lambda x: len(x.parts),
            reverse=True,
        )

        for path in paths:
            if path.is_file():
                path.unlink()
            else:
                path.rmdir()

        self.project_buffer_path.rmdir()

    def reset_project(self) -> None:
        """Delete the data_out folder and codebook_mirror.json file."""
        self._delete_project_out_folder()
        self.logger.info(
            f" -> DELETED FOLDER : 'data_out/{self.project_name}' "
        )
        self._delete_project_buffer_folder()
        self.logger.info(
            f" -> DELETED FOLDER : 'data_buffer/{self.project_name}' "
        )

    def reset_data_out(self) -> None:
        """Delete the data_out folder."""
        self._delete_project_out_folder()
        self.logger.info(
            f" -> DELETED FOLDER : 'data_out/{self.project_name}' "
        )

    def reset_data_buffer(self) -> None:
        """Delete the data_buffer folder."""
        self._delete_project_buffer_folder()
        self.logger.info(
            f" -> DELETED FOLDER : 'data_buffer/{self.project_name}' "
        )

    def reset_log(self) -> None:
        """Delete the log file."""
        if self.log_file.exists():
            self.log_file.unlink()
