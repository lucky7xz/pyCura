import polars as pl


def apply_case(data: list[tuple[str,any]] | dict[str, any] | tuple [pl.LazyFrame, str], case: str, target_values = False) -> list[tuple[str,any]] | dict[str, any]:
    def apply_case_cell(cell: str, case: str) -> str:
        if case == "upper":
            return cell.upper()
        elif case == "lower":
            return cell.lower()
        else:
            return cell

    # REVISE - WE DONT NEED THE HELPER FUNCTION - write a if in instances for performance
    assert isinstance(case, str), "case must be a string"

    if isinstance(data, list):
        return [(apply_case_cell(k, case), id) for k, id in data]
    

    

    elif isinstance(data, dict):
        if target_values == "target_values":
            return {k: apply_case_cell(v, case) for k, v in data.items()}
        else:
            return {apply_case_cell(k, case): v for k, v in data.items()}

    elif isinstance(data, tuple):
        # closure function to be able to pass column name, native polars api for performance
        if case == "upper":
            return data[0].with_columns(pl.col(data[1]).str.to_uppercase().alias(data[1]))
        elif case == "lower":
            return data[0].with_columns(pl.col(data[1]).str.to_lowercase().alias(data[1]))