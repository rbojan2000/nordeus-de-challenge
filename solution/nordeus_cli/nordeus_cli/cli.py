import click
import pandas as pd
from nordeus_cli.db_client import DBClient
from nordeus_cli.query_generator import QueryGenerator
from nordeus_cli.utils import validate_date
from tabulate import tabulate


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "--api",
    required=True,
    type=click.Choice(["user-level", "game-level"]),
    help="Data API",
)
@click.option("--user-id", required=False, help="User ID for the query")
@click.option(
    "--date",
    required=False,
    callback=validate_date,
    help="Date for the query (YYYY-MM-DD)",
)
def query(api, user_id, date):
    click.echo(click.style(f"API Endpoint: {api}", fg="bright_blue"))
    click.echo(click.style(f"User ID: {user_id}", fg="green"))

    if date:
        click.echo(click.style(f"Date: {date}", fg="yellow"))
    else:
        click.echo(click.style("Date: Not provided", fg="yellow"))

    db_client = DBClient()
    query_generator = QueryGenerator()

    query = query_generator.get_query(level=api, date=date, user_id=user_id)
    click.echo(click.style(f"Generated SQL Query: {query}", fg="cyan"))

    data = db_client.submit_query(query)

    if isinstance(data, pd.DataFrame):
        if not data.empty:
            click.echo(click.style("Query Results:", fg="magenta"))
            click.echo(tabulate(data, headers="keys", tablefmt="grid", showindex=False))
        else:
            click.echo(click.style("No data found.", fg="red"))


cli.add_command(query)

if __name__ == "__main__":
    cli()
