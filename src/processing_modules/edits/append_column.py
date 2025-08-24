import polars as pl

def append_column(data: tuple[pl.LazyFrame, str], source_column: str, regex_pattern: str):
    """
    Append a new field to the LazyFrame by extracting values from a source column.
    
    Args:
        data: Tuple of (LazyFrame, key) where key is the name of the new field
        source_column: Column to extract data from
        regex_pattern: Regular expression with a capture group
            
    Returns:
        Updated LazyFrame with the new field added
        
    Example:
        In config.json:
        {"append_field": {
            "year": ["file_name", ".*(20\\d{2}).*"]
        }}
    """

    if not isinstance(data, tuple):
        # Codebook case - not supported yet
        raise ValueError("append_column currently only supports LazyFrame data")
        
    if len(data) != 2:
        raise ValueError("append_column requires a tuple of (LazyFrame, key)")
    
    lf, new_field_name = data
    
    # Check if source column exists
    available_columns = lf.collect_schema().names()
    if source_column not in available_columns:
        raise ValueError(f"Source column '{source_column}' not found in LazyFrame. Available columns: {available_columns}")

    result = lf.with_columns(
        pl.col(source_column)
        .str.extract(regex_pattern, 1)
        .alias(new_field_name)
    )
    return result
