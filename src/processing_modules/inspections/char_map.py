

import polars as pl

def char_map(data: list | dict[str, any] | tuple[pl.LazyFrame, list[str]], target_values: bool) -> dict[str, any]:
    """get all the unique characters in the data for each key, and return a dict with the key as the key and the unique characters as the value, sorted alphabetically"""

    if isinstance(data, list):
        unique_chars = set()
        
        for value in data:
            if value:  # Check if value is not None or empty
                for char in value:
                    unique_chars.add(char)
        
        result = sorted(list(unique_chars))
        #print("res", result[:30])
        return result
    
    elif isinstance(data, dict):
        # TODO: Hardcoded ['data'] subdict - change that
        data = data["data"]
        list_chars_per_key = {}

        # get all unique characters in the data
        for key, values in data.items():
            
            if not target_values:
                list_chars_per_key[key] = []
                # each value is a string
                list_chars_per_key[key] = [char for value in values for char in value]
                # get unique characters
                list_chars_per_key[key] = list(set(list_chars_per_key[key]))
                list_chars_per_key[key].sort()

            if target_values:
                list_chars_per_key[key] = {}
                list_chars_per_key[key] = [char for value in data[key].values() for char in value]
                # get unique characters
                list_chars_per_key[key] = list(set(list_chars_per_key[key]))
                list_chars_per_key[key].sort()

        return list_chars_per_key
    
    elif isinstance(data, tuple) and len(data) == 2 and isinstance(data[0], pl.LazyFrame):
        lf, white_list = data
        char_map_dict = {}
        lf_schema = lf.collect_schema().names()
        total_cols = len(white_list)
        for col in white_list:
            if col not in lf_schema:
                continue
            import time
            start = time.time()
            
            # Assuming 'lf' is your LazyFrame and 'text' is the column of interest
            lf = lf.with_columns(
                pl.col(col).str.split("").alias("char_list")
            )
            
            # Collect the LazyFrame to execute the computation
            #df = lf.select("char_list").collect()

            # Flatten the list of lists into a single list
            #all_chars = [char for sublist in df["char_list"] for char in sublist]

            # Get unique characters and sort them
            #unique_chars = sorted(set(all_chars))
        
            
            char_lf = (
                lf
                .select(
                    pl.col(col)
                    #.drop_nulls()
                    .str.split("")
                    .explode()
                    .unique()
                    .sort()
                    .alias("unique_chars")
                )
                .collect()
            )
                    # 1. pl.col(col): Select the column by name.
                    # 2. drop_nulls(): Remove any null values (if present) to avoid errors.
                    # 3. str.split(""): Split each string into a list of its characters.
                    # 4. explode(): Turn each character in the list into its own row (flatten).
                    # 5. unique(): Get the unique characters only (removes duplicates).
                    # 6. sort(): Sort the unique characters alphabetically.
                    # 7. alias("unique_chars"): Name the resulting column 'unique_chars'.
    


            # if chages were mate to the lf, (which is very likely)
            # this will
            unique_chars = char_lf["unique_chars"].to_list()
            
            #print(unique_chars) # does this print diff from the one below?

            char_map_dict[col] = unique_chars
            total_cols -= 1
            print(f"\n char_map for {col} {time.time() - start:.2f}s. {total_cols} left")
            print(char_map_dict[col])
        return char_map_dict

    else:
        raise ValueError("Unsupported data type")


