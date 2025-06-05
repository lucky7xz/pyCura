import polars as pl

def apply_char_replace(data: list[tuple[str,any]] | dict[str, any] | tuple[pl.LazyFrame, str], char_replace: list[list[str, str]], target_values = False) -> list[tuple[str,any]] | dict[str, any]:
    def apply_char_replace_cell(cell: str, char_replace: list[list[str, str]]) -> str:
        

        for  i in range(len(char_replace)):

            if not char_replace[i][0] == "":
                cell = cell.replace(char_replace[i][0], char_replace[i][1])
            
            else:
               raise ValueError(f'Editting empty cells ("") have to be eddited with token_replace module')
            
    
        return cell
    
    assert isinstance(char_replace, list), "char_replace must be a list of 2-element lists"
    assert isinstance(char_replace[0], list), "char_replace must be a list of 2-element lists"
    assert len(char_replace[0]) == 2, "char_replace must be a list of 2-element lists"
    

    if isinstance(data, list):
        return [(apply_char_replace_cell(k, char_replace), id) for k, id in data]


    elif isinstance(data, dict):
        if target_values == "target_values":
            return {k: apply_char_replace_cell(v, char_replace) for k, v in data.items()}
        else:
            return {apply_char_replace_cell(k, char_replace): v for k, v in data.items()}

    elif isinstance(data, tuple):
        # closure function to be able to pass column name, native polars api for performance

        list_of_edits = char_replace

        # Apply all replacements in sequence using .str.replace for each pair, allow substring replace
        expr = pl.col(data[1])
        for from_token, to_token in list_of_edits:
            expr = expr.str.replace(from_token, to_token, literal=True)
        return data[0].with_columns(expr.alias(data[1]))
    