import argparse

from src.processors.codebook_processor import CodebookProcessor
from src.processors.domain_data_processor import DomainDataProcessor

from src.shared.project_manager import ProjectManager


def main():
    
    parser = argparse.ArgumentParser(
        description="pyCura CLI for project management"
    )
    parser.add_argument(
        "config_filename", type=str, help="Path to the configuration file"
    )
    subparsers = parser.add_subparsers(
        dest="command", help="Available commands"
    )

    # Define subparsers for each command
    subparsers.add_parser("parsecb", help="Parse codebook")
    subparsers.add_parser("parsedd", help="Parse domain data")
    subparsers.add_parser("reset", help="Reset project folder")
    subparsers.add_parser("resetlog", help="Reset log file")
    subparsers.add_parser("cbinspection", help="Run codebook inspection")
    subparsers.add_parser("ddinspection", help="Run domain inspection")
    subparsers.add_parser("run", help="Run full processing")

    # Parse arguments
    args = parser.parse_args()

    # If no command is provided, show help and exit
    if args.command is None:
        parser.print_help()
        return

    # CONFIG FILE EXTENSION
    config_filename = args.config_filename



    project_manager = ProjectManager(config_filename)

    try:
        match args.command:
            case "parsecb":
                parsecb(project_manager)
            case "parsedd":
                parsedd(project_manager)
            case "cbinspection":
                cbinspection(project_manager)
            case "ddinspection":
                ddinspection(project_manager)
            case "run":
                run(project_manager)
            case "reset":
                reset(project_manager)
            case "resetlog":
                resetlog(project_manager)

    except KeyboardInterrupt:
        
        kb_exit = input("\n> Process interrupted by user. Press y and Enter to reset all output and/or buffer files for this project, or Press Enter to exit...")
        if kb_exit.lower() == "y":

            # Press o and enter to reset the output files, b to reset the buffer files, or both and enter to reset both 
            reset_type = input("\n> Press o and enter to reset the output files, both to reset both output and buffer, or Press Enter to exit...")
            if reset_type.lower() == "o":
                project_manager.reset_data_out()
                print("Output files deleted successfully.")
            elif reset_type.lower() == "b":
                project_manager.reset_data_buffer()
                print("Buffer files deleted successfully.")
            elif reset_type.lower() == "both":
                project_manager.reset_project()
                print("Project files deleted successfully.")
            else:
                print("\n> Invalid option. Exiting...")
                exit()
        else:
            print("\n> Process interrupted by user. Continuing...")
            exit()

# --------------- STEP-FUNCTIONS ---------------
def parsecb(project_manager):
    codebook_processor = CodebookProcessor(project_manager.cb_injection)
    codebook_processor.run_codebook_pre_processing()

#TODO Reset inspections
def cbinspection(project_manager):
    codebook_processor = CodebookProcessor(project_manager.cb_injection)
    codebook_processor.run_inspection_processing(
        project_manager.post_transformation
    )

def parsedd(project_manager):
    domain_processor = DomainDataProcessor(project_manager.dd_injection)
    domain_processor.run_domain_pre_processing()

#TODO Reset inspections
def ddinspection(project_manager):
    domain_processor = DomainDataProcessor(project_manager.dd_injection)
    domain_processor.run_inspection_processing(
        project_manager.post_transformation
    )

#--------------------------------------------------
def reset(project_manager):
    reset_type = input(
        f"\n> What do you want to reset for project '{project_manager.project_name}'?"
        "\n> 1. Only data output\n> 2. Entire project (data output and buffer)"
        "\n> Enter 1 or 2: "
    )

    if reset_type == "1":
        confirmation = input(
            f"\n> Are you sure you want to reset ONLY the data output for '{project_manager.project_name}'? (y/n): "
        )
        if confirmation.lower() == "y":
            project_manager.reset_data_out()
            project_manager.logger.info(" -> DATA OUTPUT RESET COMPLETED")
        else:
            project_manager.logger.warning(" -> DATA OUTPUT NOT RESET")
    elif reset_type == "2":
        confirmation = input(
            f"\n> Are you sure you want to reset the ENTIRE project '{project_manager.project_name}'?\n> This will delete all output AND buffer files for this project. (y/n): "
        )
        if confirmation.lower() == "y":
            project_manager.reset_project()
            project_manager.logger.info(" -> COMPLETE PROJECT RESET COMPLETED")
        else:
            project_manager.logger.warning(" -> PROJECT NOT RESET")
    else:
        print("\n> Invalid option. Please enter 1 or 2.")
        project_manager.logger.warning(" -> RESET ABORTED: INVALID OPTION")

#safe
def resetlog(project_manager):
    confirmation = input(
        "\n> Do you sure you want to reset the log file? (y/n): "
    )
    if confirmation.lower() == "y":
        project_manager.reset_log()
        project_manager.logger.info(" -> LOG FILE RESET COMPLETED")
    else:
        project_manager.logger.warning(" -> LOG FILE NOT RESET")


def run(project_manager):
    #  ----- INITIALIZING -----
    target_data_structures = input("\n> What target(s) to inspect? (cb/dd/both): ")
    
    if target_data_structures not in ["cb", "dd", "both"]:
        raise ValueError(f"Invalid input '{target_data_structures}'. Use 'cb', 'dd', or 'both'")

    cb_injection = project_manager.cb_injection
    dd_injection = project_manager.dd_injection
    
    # Initialize only the necessary processors
    #codebook_processor, domain_processor = target_init(target_data_structures)
    match target_data_structures.lower():
        case "cb":
            codebook_processor = CodebookProcessor(cb_injection)
        case "dd":
            domain_processor = DomainDataProcessor(dd_injection)
        case "both":
            codebook_processor = CodebookProcessor(cb_injection)
            domain_processor = DomainDataProcessor(dd_injection)

    # Helper Functions
    #  -------------------- TARGETING FUNCTIONS -----------------------------
    def target_preprocessing(target_data_structures):
        match target_data_structures.lower():
            case "cb":
                codebook_processor.run_codebook_pre_processing()
            case "dd":
                domain_processor.run_domain_pre_processing()
            case "both":
                codebook_processor.run_codebook_pre_processing()
                domain_processor.run_domain_pre_processing()

    def target_inspection(target_data_structures, skip_inspection):
        match target_data_structures.lower():
            case "cb":
                codebook_processor.run_inspection_processing(skip_inspection)
            case "dd":
                domain_processor.run_inspection_processing(skip_inspection)
            case "both":
                codebook_processor.run_inspection_processing(skip_inspection)
                domain_processor.run_inspection_processing(skip_inspection)
                #project_manager.logger.info(domain_processor.parsed_table.explain())

    def target_edits(target_data_structures, key, edit_function, parameters):
        """Route edit operations based on target and handle value/non-value keys."""

        if key.replace("-values", "") in project_manager.config["white_list"] \
            or (edit_function == "append_column") \
            or (key == "pyCura_id" and project_manager.dd_injection["parsing_options"]["add_id"]):
        
            match target_data_structures.lower():
                case "dd":
                    if "-values" not in key: #should not be here
                        domain_processor.run_edit(
                            key, edit_function, parameters
                        )
                    else:
                        #key = key.replace("-values", "")
                        pass
                        

                case "cb":
                    if "-values" in key:
                        parameters.append("target_values")
                        key = key.replace("-values", "")
                    codebook_processor.run_edit(key, edit_function, parameters)

                case "both":
                    if "-values" in key:
                        parameters.append("target_values")
                        key = key.replace("-values", "")
                        codebook_processor.run_edit(
                            key, edit_function, parameters
                        )
                        # since the domain data does not contain values, 
                        # we can skip the domain edit
                        # the non-value case is handled below
                    else:
                        domain_processor.run_edit(
                            key, edit_function, parameters
                        )
                        codebook_processor.run_edit(
                            key, edit_function, parameters
                        )

        else:
            project_manager.logger.warning(
                f" -> INVALID EDIT TARGET: {key} NOT IN WHITE LIST. SKIPPING EDIT..."
            )
    #  ------------- RUN PARSING AND INSPECTIONS -----------------
    
    target_preprocessing(target_data_structures)
    
    second_run = False
    run_inspection = input("\n\n> Press 'y' and enter to run initial inspections. Press Enter to skip.")
    
    if run_inspection.lower() == "y":

        #explain parsed_table
        #project_manager.logger.info("\n\n --- EXPLAINING Parsed Table ---")
        
        
        target_inspection(target_data_structures, second_run)
        second_run = True
    
    #else
        # we still need to parse the domain data
        
    
    project_manager.logger.info("\n\n --- RUNNING EDITS ---")


    #--------------------------------------------------------------------------
    #--------------------------------------------------------------------------
    
    
    #  ----- PREPARING EDITS -----
    white_list = project_manager.config["white_list"]
    for i in range(len(project_manager.config["edits"])):
        for edit_function, keys in project_manager.config["edits"][i].items():  
            
            keys_to_edit = {}
            keys_to_edit = keys.copy()
            non_all_keys_swtich = False

            if "all_keys" in keys:
                parameters = keys["all_keys"]
                keys_to_edit.update(all_keys_edit(white_list, parameters))
                keys_to_edit.pop("all_keys")

                non_all_keys_swtich = True
                keys.pop("all_keys")

            if "all_values" in keys:
                parameters = keys["all_values"]
                keys_to_edit.update(all_values_edit(white_list, parameters))
                keys_to_edit.pop("all_values")

                non_all_keys_swtich = True
                keys.pop("all_values")

            # Some edits are lost here. we need to add them back in
            if non_all_keys_swtich and keys:
                keys_to_edit.update(keys)
                # print("\n\nEDITS: ", keys)

            project_manager.config["edits"][i][edit_function] = keys_to_edit
            # print("\n\nEDITS: ", project_manager.config['edits'][i][edit_function])
    
    
    #  ----- RUNNING EDITS -----
    for i in range(len(project_manager.config["edits"])):
        for edit_function, keys in project_manager.config["edits"][i].items():
            
            print("\n")
            # print(i, edit_function) #DEBUGGING

            for key in keys:
                project_manager.logger.info(
                    f"RUNNING EDIT: {edit_function} for key {key}..."
                )

                parameters = []
                parameters[:] = project_manager.config["edits"][i][edit_function][key]
                # was this actually necessary?

                project_manager.logger.info(f"Parameters: {parameters}")
                
                # Run edits based on target input(s). Functions are defined above
                if target_data_structures.lower() in ["cb", "dd", "both"]:
                    target_edits(
                        target_data_structures.lower(), key, edit_function, parameters
                    )

                #DEBUGGING
                #print(type(domain_processor.parsed_table))
                #print(domain_processor.parsed_table.head().collect())
    #------------------------------------------------------------------------------
    #------------------------------------------------------------------------------

    #  ----- RUNNING INSPECTIONS (POST-TRANSFORMATION) -----
    project_manager.logger.info(" --- EDITING COMPLETED ---\n")
    project_manager.post_transformation = True

    if target_data_structures.lower() in ["dd", "both"]:
        domain_processor.print_edited_table_sample()

    skip_inspection_again = input("> Press 'y' and Enter to run post-transformation inspections. Press Enter to skip.")
    if skip_inspection_again.lower() == "y":
        
        #if domain_processor: # FOR DEBUGGING
        #    print(domain_processor.parsed_table.explain())
            
        target_inspection(target_data_structures, second_run)
    project_manager.logger.info(" --- PROJECT PROCESSING COMPLETED ---")

    #--------------------------------------------------------------------------

    #  ----- EXPORTING -----
    # only export the datastructures targeted by the user
    if target_data_structures.lower() in ["cb", "both"]:
        
        if (input("\n> Would you like to export the codebook keys? (y/n): ").lower() == "y"):
            codebook_processor.run_export()
            project_manager.logger.info(" -> CODEBOOK KEYS EXPORTED")
        else:
            project_manager.logger.info(" -> NO CODEBOOK KEYS EXPORTED")

    if target_data_structures.lower() in ["dd", "both"]:
        if (input("\n> Would you like to export the domain data? (y/n): ").lower() == "y"):
            domain_processor.run_export()
            project_manager.logger.info(" -> DOMAIN DATA EXPORTED")
        else:
            project_manager.logger.info(" -> NO DOMAIN DATA EXPORTED")


# ---------------------------------------------------------------------------------------------



#  ------------ TO BE MOVED ------------
def all_keys_edit(white_list, parameters):
    """Generate a dictionary of all keys in the white_list with the given parameters"""
    return {white_list_key: parameters for white_list_key in white_list}


def all_values_edit(white_list, parameters):
    """Generate a dictionary of all keys (with value routing) in the white_list with the given parameters"""
    return {
        white_list_key + "-values": parameters for white_list_key in white_list
    }


if __name__ == "__main__":
    main()
