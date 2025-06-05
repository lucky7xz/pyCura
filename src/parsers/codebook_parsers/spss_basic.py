import re

"""
SPSS Basic Codebook Parser

This module parses SPSS codebook text files and returns a structured dictionary 
containing metadata and data sections for each key in the codebook.

The output dictionary has two main sections:
- metadata: Contains a dictionary mapping each key to its associated metadata
- data: Contains a dictionary mapping each key to its associated data values 
and labels
"""


def split_by_keys(codebook_text: str) -> list[tuple[str, str]]:
    """Split the source text data by keys using a regular expression to
    identify sections."""

    def check_section(key: str, section_text: str) -> bool:
        # Simple length check
        MIN_SECTION_LENGTH = 100  # Adjust this value as necessary
        if len(section_text) < MIN_SECTION_LENGTH:
            raise ValueError(
                f"Section '{key}' is too short (length {len(section_text)})."
                + f" Minimum required length is {MIN_SECTION_LENGTH}."
            )

        return True

    source_data = codebook_text
    assert isinstance(source_data, str), (
        "Source data to be parsed must be of type string"
    )

    # German + English regex. 
    key_pattern = re.compile(r"\|\n\n^[A-Za-z0-9_ÜÄÖ+ß]+$\n\|", re.MULTILINE)
    keys = [
        (match.group(), match.start())
        for match in key_pattern.finditer(source_data)
    ]

    sections = []

    for i in range(len(keys)):
        key, start_pos = keys[i]
        key = key.replace("|", "").strip()
        end_pos = keys[i + 1][1] if i + 1 < len(keys) else len(source_data)
        section_text = source_data[start_pos:end_pos].strip()

        # Use the helper function to check the section
        check_section(key, section_text)
        sections.append((key, section_text))

    return sections


def parse_line(line: str) -> list[str]:
    """
    Parses a single line from the section text and extracts data fields.

    Relies on the consistent structure of SPSS export files where data is
    presented in tables with vertical bar separators, regardless of the
    language.
    """
    assert isinstance(line, str), "Line to be parsed must be of type string"

    # TODO: Make the checking stricter

    # Skip pure separator lines (like |-------|-------|)
    if line.count("-") > 10 and "|" in line:
        return []

    # Process data lines with vertical bar separators
    if line.startswith("|") and "|" in line[1:]:
        # Split by vertical bar and clean up each part
        line_parts = [part.strip() for part in line.split("|")]
        assert len(line_parts) == 4 or len(line_parts) == 5, (
            f"Line must have 4 or 5 parts when split (by |): {line_parts}"
        )

        line_pattern = [1 if part != "" else 0 for part in line_parts]
        # print("\n\nsplit", line_parts)
        # print("part_pattern", line_pattern,"\n")

        match sum(line_pattern):
            case 0:
                return []
            case 1:
                return []
            case 2:
                if len(line_pattern) == 5 and line_parts[2] == "":
                    return [line_parts[1], line_parts[2], line_parts[3]]
                else:
                    return [line_parts[2], line_parts[3]]
            case 3:
                return [line_parts[1], line_parts[2], line_parts[3]]
            case _:
                raise ValueError(
                    f"Invalid line_pattern: {line_pattern} for {line_parts}"
                )


def parse_section(section_string: str, logger) -> dict:
    """
    Parses a section of text and extracts metadata and key-value pairs.

    Uses the predictable structure of SPSS output files to identify
    metadata and value sections without relying on specific keywords.
    """
    assert isinstance(section_string, str), (
        "Section string must be of type string"
    )
    to_print = section_string[:10].replace("\n", "")
    logger.info(f"Parsing section starting with: {to_print}...")

    # Initialize the dictionaries
    section_data = {}
    section_metadata = {}

    # Split the section into lines and parse
    lines = [line.strip() for line in section_string.splitlines()]
    parsed_lines = [parse_line(line) for line in lines]
    parsed_lines = [
        line for line in parsed_lines if line
    ]  # Filter empty results
    # Check if we have enough data to process
    if len(parsed_lines) < 3:
        logger.warning(
            f"Section too short for valid processing: {section_string[:50]}..."
        )
        raise ValueError(
            f"Section too short for valid processing: {section_string[:50]}..."
        )

    debug_switch = False
    meta_vs_data_switch = 0

    for line in parsed_lines:
        if len(line) < 2:
            if debug_switch:
                print(line, " SKIPPED")
            continue

        # if debug_switch:
        #    print(line)

        match (meta_vs_data_switch, len(line)):
            # starting metadata section (1st line with len(3))
            case (0, 3):
                if debug_switch:
                    print(line, " METADATA_START")

                meta_vs_data_switch += 1

                third_tag = line[0]
                key = line[1]
                value = line[2]

                section_metadata["meta_tag"] = third_tag
                section_metadata[key] = value
            # starting data section (2nd line with len(3))
            case (1, 3):
                if debug_switch:
                    print(line, " DATA_START")
                meta_vs_data_switch += 1
                third_tag = line[0]
                key = line[1]
                value = line[2]
                section_metadata["value_tag"] = third_tag
                section_data[key] = value

            case (2, 3):
                if debug_switch:
                    print(line, "MORE METADATA?!")
                logger.warning(
                    f"More metadata found: {line}. Creating new key in metadata sub-dictionary."
                )
                logger.warning(f"Current metadata: {section_string}")
                key = line[1]
                value = line[2]
                if line[0] in section_metadata.keys():
                    section_metadata[line[0]].append({key: value})
                else:
                    section_metadata[line[0]] = [{key: value}]

            # regular metadata key-value pairs
            case (1, 2):
                if debug_switch:
                    print(line, " METADATA_KEY_VALUE")
                key = line[0]
                value = line[1]
                section_metadata[key] = value

            # regular data key-value pairs
            case (2, 2):
                if debug_switch:
                    print(line, " DATA_KEY_VALUE")
                key = line[0]
                value = line[1]
                section_data[key] = value

            case _:
                raise ValueError(
                    f"Invalid match/switch values: {meta_vs_data_switch}, {len(line)}"
                )

    return section_data, section_metadata


def parse_codebook(codebook_path: str, logger) -> dict[str, dict[str, any]]:
    """Parse the entire codebook into a structured dictionary."""

    logger.info("Parsing .txt Codebuch into .json dictonary...")

    with open(codebook_path, "r", encoding="utf-8") as file:
        codebook_text = file.read()

    # Check if the codebook is valid
    assert isinstance(codebook_text, str), "Codebook is not a string"
    assert codebook_text.strip(), "Codebook is empty"
    # Assert codebook_text is spss exported codebook
    assert "Codebuch\nHinweise" in codebook_text, (
        "File is not an SPSS Codebook (missing header)"
    )

    # Split the data by keys
    sections = split_by_keys(codebook_text)
    logger.info(f"Found {len(sections)} sections in the codebook")
    parsed_codebook = {}
    parsed_codebook["data"] = {}
    parsed_codebook["metadata"] = {}

    # Parse each section individually
    for key, section_text in sections:
        section_data, section_metadata = parse_section(section_text, logger)
        parsed_codebook["data"][key] = section_data
        parsed_codebook["metadata"][key] = section_metadata

    return parsed_codebook
