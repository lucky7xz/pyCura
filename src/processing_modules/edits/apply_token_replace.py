import polars as pl
import re

def apply_token_replace(data: list[tuple[str,any]] | dict[str, any] | tuple[pl.LazyFrame, str], tok_replace: list[list[str, str]], target_values = False) -> list[tuple[str,any]] | dict[str, any]:
    def apply_token_replace_cell(cell: str, tok_replace: list[list[str, str]]) -> str:
        # Check if the cell exactly matches any token to be replaced
        
        for i in range(len(tok_replace)):
            
            if cell == tok_replace[i][0]:
                cell = tok_replace[i][1]
        
        # If no match found, return the original cell
        return cell
    
    assert isinstance(tok_replace, list), "tok_replace must be a list of 2-element lists"
    assert isinstance(tok_replace[0], list), "tok_replace must be a list of 2-element lists"
    assert len(tok_replace[0]) == 2, "tok_replace must be a list of 2-element lists"
    
    if isinstance(data, list):
        return [(apply_token_replace_cell(k, tok_replace), id) for k, id in data]

    elif isinstance(data, dict):
        if target_values == "target_values":
            return {k: apply_token_replace_cell(v, tok_replace) for k, v in data.items()}
        else:
            return {apply_token_replace_cell(k, tok_replace): v for k, v in data.items()}


    elif isinstance(data, tuple):
        # closure function to be able to pass column name, native polars api for performance


        # Problem 1, causes errors (i think)
        # Problem 2, should only replaces tokens (not substrings, eg '1010' in '1010101' is false positive here). 
        # --> use regex to match only exact tokens
        list_of_edits = tok_replace
        
        expr = pl.col(data[1])
        for from_token, to_token in list_of_edits:

            pattern = f"^{re.escape(from_token)}$"
            expr = expr.str.replace(pattern, to_token, literal=False)
        return data[0].with_columns(expr.alias(data[1]))