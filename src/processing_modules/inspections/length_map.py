import polars as pl

# TODO: REVISE
def length_map(data: list | dict[str, any] | tuple[pl.LazyFrame, str], target_values: bool) -> dict[str, any]:
    """
    Extract a key-length map from a dataframe or dictionary.

    For a DataFrame:
    Takes a DataFrame and returns a dict where:
    - Keys are column names from the DataFrame
    - Values are dicts mapping string lengths to their frequency in that column
    
    For example, given DataFrame:
        col1
        'a'      # len 1
        'bb'     # len 2
        'ccc'    # len 3
        'bb'     # len 2
    
    Returns:
        {
            'col1': {
                1: 1,  # one string of length 1
                2: 2,  # two strings of length 2  
                3: 1   # one string of length 3
            }
        }

    For a dictionary:
    Takes a dict and returns a dict where:
    - Keys are the original dict keys
    - Values are dicts mapping string lengths to their frequency in the values

    For dictionaries, we expect a nested structure where each key has a "data" subdict
        The values subdict contains key-value pairs where the keys are codes (e.g. "000", "010")
        and the values are their descriptions. We want to count the lengths of these codes.
        For example, given:
        {
           "col1": {
               "": "no info",
               "000": "Germany", 
               "010": "Saarland"
           }
        }
        We would count: {"": 1, "000": 1, "010": 1} to get the frequency of each key length
        
    """

    if isinstance(data, list):
        # For each column, count the lengths of values and store in a dictionary
        length_map = {}
        for value in data:
            value_len = len(value)
            if value_len not in length_map:
                length_map[value_len] = 0
            length_map[value_len] += 1
            
        return length_map

    elif isinstance(data, dict):
        length_map = {}
        subdict = data["data"]

        #   --- Targeting Keys ---
        if not target_values:
            for key, values in subdict.items():
                if not values:
                    length_map[key] = {"null": "null"}
                else:
                    length_map[key] = {}
                    for value in values:
                        
                        
                        value_len = len(value)  # Calculate length once
                        if value_len not in length_map[key]:
                            length_map[key][value_len] = 0
                        length_map[key][value_len] += 1
        
        #   --- Targeting Values ---
        else:
            for key, values in subdict.items():
                if not values:
                    length_map[key] = {"null": "null"}
                else:
                    length_map[key] = {}
                    for value in values.values():
                        
                        
                        value_len = len(value)  # Calculate length once
                        if value_len not in length_map[key]:
                            length_map[key][value_len] = 0
                        length_map[key][value_len] += 1
        
        return length_map


    elif isinstance(data, tuple):
        lf, white_list = data
        length_map = {}
        total_cols = len(white_list)
        lf_schema = lf.collect_schema().names()
        # Count string lengths in the LazyFrame column(s)
        for column in white_list:
            if column not in lf_schema:
                continue
            import time
            start = time.time()
            counts_df = (
                lf
                .select(pl.col(column).str.len_chars().alias("len"))
                .group_by("len")
                .agg(pl.count().alias("counts"))
                .sort("len")
                .collect()
            )
            length_map[column] = dict(
                zip(counts_df["len"].to_list(), counts_df["counts"].to_list())
            )
            total_cols -= 1
            print(f"\n length_map for {column} {time.time() - start:.2f}s. {total_cols} left")
            print(length_map[column])
        return length_map
        

    else:
        raise ValueError("Unsupported data type")