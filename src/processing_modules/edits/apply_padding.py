import polars as pl


def apply_padding(
    data: list[tuple[str, int]] | dict[str, any] | tuple[pl.LazyFrame, str],
    length: str,
    token: str,
    target_values=False,
) -> list[tuple[str, any]] | dict[str, any]:
    def apply_padding_cell(cell: str, length: str, token: str) -> str:
        return token * (int(length) - len(cell)) + cell

    assert isinstance(length, str), "Length must be a string"
    assert isinstance(token, str), "Token must be a string"

    # THIS WAS USED FOR SQLITE BUT IS TECHNICALLY DEPRECATED - ALL applies ARE NEW IN POLARS
    if isinstance(data, list):
        # Even though the tuples in python are immutable, a list comprehension allows us replace the tuples with new tuples
        return [(apply_padding_cell(k, length, token), id) for k, id in data]

    elif isinstance(data, dict):
        if target_values == "target_values":
            return {
                k: apply_padding_cell(v, length, token)
                for k, v in data.items()
            }
        else:
            return {
                apply_padding_cell(k, length, token): v
                for k, v in data.items()
            }

    elif isinstance(data, tuple):
        # TODO: PAD START/END DISTINCTION
        return data[0].with_columns(pl.col(data[1]).str.pad_start(int(length), token).alias(data[1]))