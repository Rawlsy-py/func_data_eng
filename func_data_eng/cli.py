import typer
from .transformer import process_json_to_parquet

app = typer.Typer(help="CLI tool to transform JSON to Parquet.")


@app.command()
def transform(
    input_json: str = typer.Argument(..., help="Path to the input JSON file."),
    output_parquet: str = typer.Argument(..., help="Path to the output Parquet file."),
):
    """
    Transforms a JSON file into a Parquet file with Polars.
    """
    try:
        typer.echo(f"Transforming {input_json} to {output_parquet}...")
        process_json_to_parquet(input_json, output_parquet)
        typer.secho("Transformation complete!", fg=typer.colors.GREEN)
    except Exception as e:
        typer.echo(f"Transforming {input_json} to {output_parquet}...")
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(transform)
