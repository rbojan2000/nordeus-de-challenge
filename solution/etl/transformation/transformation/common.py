from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from transformation.paths import CURATED_DATASET_PATH, EVENTS_DATASET_PATH


def save_dataset(dataset: DataFrame, name: str, partition_col: str = None) -> None:
    dataset.write.parquet(path=f"{str(CURATED_DATASET_PATH)}/{name}")


def read_and_process_data(
    spark: SparkSession,
    schema: StructType,
    event_type: str,
    additional_filters=None,
) -> DataFrame:
    """General function to read, filter, and select data with common operations."""

    df = (
        spark.read.json(str(EVENTS_DATASET_PATH), schema=schema)
        .dropDuplicates(["event_id"])
        .filter(col("event_type") == event_type)
        .filter(
            col("event_id").isNotNull()
            & col("event_type").isNotNull()
            & col("event_timestamp").isNotNull()
        )
    )

    if additional_filters:
        df = df.filter(additional_filters)
    return df
