from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, when
from transformation.common import read_and_process_data
from transformation.schema import match_schema, registration_schema, session_schema


def transform_registration(spark: SparkSession, dataset: str) -> DataFrame:
    df = (
        read_and_process_data(spark, registration_schema, dataset)
        .select(
            col("event_id"),
            col("event_data.user_id").alias("user_id"),
            col("event_data.country").alias("country"),
            col("event_data.device_os").alias("device_os"),
            from_unixtime(col("event_timestamp")).alias("registration_timestamp"),
        )
        .orderBy("registration_timestamp")
    )

    return df


def transform_session(spark: SparkSession, dataset: str) -> DataFrame:
    df = (
        read_and_process_data(spark, session_schema, dataset)
        .select(
            col("event_id"),
            col("event_data.user_id").alias("user_id"),
            col("event_data.type").alias("type"),
            from_unixtime(col("event_timestamp")).alias("session_timestamp"),
        )
        .orderBy("session_timestamp")
    )

    return df


def transform_match(spark: SparkSession, dataset: str) -> DataFrame:
    df = (
        read_and_process_data(spark, match_schema, dataset)
        .select(
            col("event_id"),
            col("event_data.match_id").alias("match_id"),
            col("event_data.home_user_id").alias("home_user_id"),
            col("event_data.away_user_id").alias("away_user_id"),
            col("event_data.home_goals_scored").alias("home_goals_scored"),
            col("event_data.away_goals_scored").alias("away_goals_scored"),
            from_unixtime(col("event_timestamp")).alias("match_timestamp"),
        )
        .withColumn(
            "match_status",
            when(
                col("home_goals_scored").isNull() & col("away_goals_scored").isNull(),
                "match_start",
            ).otherwise("match_end"),
        )
        .orderBy("match_timestamp")
    )

    return df
