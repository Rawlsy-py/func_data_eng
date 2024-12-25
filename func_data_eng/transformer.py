import polars as pl


def read_json(file_path: str) -> pl.DataFrame:
    """Reads a JSON file into a Polars DataFrame."""
    return pl.read_json(file_path)


def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transforms the data by applying some functional operations."""
    return (
        df.with_columns(
            [pl.col("price").cast(pl.Float64), pl.col("quantity").cast(pl.Int64)]
        )
        .filter(pl.col("price") > 0)
        .select(
            [
                "item_id",
                "price",
                "quantity",
                (pl.col("price") * pl.col("quantity")).alias("total_value"),
            ]
        )
    )


def write_parquet(df: pl.DataFrame, output_path: str) -> None:
    """Writes the Polars DataFrame to a Parquet file."""
    df.write_parquet(output_path)


def process_json_to_parquet(input_json: str, output_parquet: str) -> None:
    """Processes JSON data to Parquet format."""
    df = read_json(input_json)
    transformed_df = transform_data(df)
    write_parquet(transformed_df, output_path=output_parquet)
