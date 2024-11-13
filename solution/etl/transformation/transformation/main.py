import click
from pyspark.sql import DataFrame, SparkSession
from transformation.common import save_dataset
from transformation.processor import (
    transform_match,
    transform_registration,
    transform_session,
)


@click.command()
@click.option(
    "--dataset-name",
    required=True,
    type=click.Choice(["session_ping", "registration", "match"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(dataset_name: str) -> None:
    spark = SparkSession.builder.appName("transformation").getOrCreate()

    if dataset_name == "session_ping":
        df = transform_session(spark, dataset_name)
    elif dataset_name == "registration":
        df = transform_registration(spark, dataset_name)
    elif dataset_name == "match":
        df = transform_match(spark, dataset_name)
    else:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

    save_dataset(dataset=df, name=dataset_name)

    spark.stop()


if __name__ == "__main__":
    run()
