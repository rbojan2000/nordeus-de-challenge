import click
from load.analyzer import Analyzer
from load.constants import APP_NAME
from load.db_client import DBClient
from load.paths import POSTGRESQL_JAR_PATH
from load.utils import get_table_name_by_job
from pyspark.sql import SparkSession


@click.command()
@click.option(
    "--job",
    required=True,
    type=click.Choice(["match-stats", "user-session-stats"], case_sensitive=False),
    help="Specify the job",
)
def run(job: str) -> None:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.jars", POSTGRESQL_JAR_PATH)
        .getOrCreate()
    )

    analyzer = Analyzer(spark)
    client = DBClient()

    if job == "match-stats":
        df = analyzer.calculate_match_stats()

    elif job == "user-session-stats":
        df = analyzer.calculate_session_stats()

    df.show()
    table_name = get_table_name_by_job(job)
    client.write(dataset = df, table_name = table_name)

    spark.stop()


if __name__ == "__main__":
    run()
