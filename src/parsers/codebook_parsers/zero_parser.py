import json


def parse_codebook(codebook_path: str, logger) -> dict:
    """
    Simply loads and returns the JSON codebook without any parsing.

    Args:
        codebook_text (str): Path to the JSON codebook file
        logger: Logger instance for logging messages

    Returns:
        dict: The loaded JSON codebook
    """

    # Assert that the file is JSON
    assert str(codebook_path).endswith(".json"), (
        "Codebook file must be JSON format"
    )
    assert codebook_path.exists(), (
        "Codebook file does not exist"
    )

    with open(codebook_path, "r") as f:
        parsed_codebook = json.load(f)
    return parsed_codebook
