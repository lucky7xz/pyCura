import polars as pl
def occurrence_map(data: list | tuple[pl.LazyFrame, list[str]], target_values: bool) -> dict:
    """
    Count the occurrences of each unique value in a list.
    
    Args:
        data: A list of values to count occurrences for
        target_values: Not used in this inspection
    Returns:
        A dictionary mapping each unique value to its number of occurrences
        
    Example:
        Given: ["Germany", "France", "Germany", "Spain"]
        Returns: {"Germany": 2, "France": 1, "Spain": 1}
    """
    if isinstance(data, list):
        
    
        occurrence_dict = {}
        
        for value in data:
            if value not in occurrence_dict:
                occurrence_dict[value] = 0
            occurrence_dict[value] += 1
    
    elif isinstance(data, tuple):
        
        lf, white_list = data
        occurrence_dict = {}
        lf_schema = lf.collect_schema().names()
        total_cols = len(white_list)
        for col in white_list:
            if col not in lf_schema:
                continue
            import time
            start = time.time()
            # Count occurrences of each unique value in the column
            counts_df = (
                lf
                .select(pl.col(col).alias("value"))
                .group_by("value")
                .agg(pl.count().alias("count"))
                .sort("value")
                .collect()
            )
            occurrence_dict[col] = dict(
                zip(counts_df["value"].to_list(), counts_df["count"].to_list())
            )
            total_cols -= 1
            print(f"\n occurrence_map for {col} {time.time() - start:.2f}s. {total_cols} left")
            print(occurrence_dict[col])
        
    return occurrence_dict
