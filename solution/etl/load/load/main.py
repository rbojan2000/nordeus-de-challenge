import click
from load.constants import APP_NAME
from load.aggregator import Aggregator
from pyspark.sql import SparkSession


@click.command()
@click.option(
    "--job",
    required=True,
    type=click.Choice(["match-stats", "user-session-stats"], case_sensitive=False),
    help="Specify the job",
)
def run(job: str) -> None:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    aggregator = Aggregator(spark)
    if job == "match-stats":
        df = aggregator.calculate_match_stats()

    elif job == "user-session-stats":
        df = aggregator.calculate_session_stats()

    df.show()

    spark.stop()


if __name__ == "__main__":
    run()
