from load.paths import (
    MATCH_DATASET,
    REGISTRATION_DATASET,
    SESSION_DATASET,
    TIMEZONE_DEFINITION_DATASET,
)
from load.constants import PING_TRESHOLD_SEC
from load.schema import match_schema, registration_schema, session_schema
from load.joiner import Joiner
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lag, unix_timestamp, when, sum
from pyspark.sql.window import Window


class Analyzer:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.joiner = Joiner()
        self.ping_treshold_sec = PING_TRESHOLD_SEC

    def calculate_session_stats(self) -> DataFrame:
        window_user_spec = Window.partitionBy("user_id").orderBy("session_timestamp")

        sessions_df = self.spark.read.parquet(str(SESSION_DATASET), schema=session_schema)
        timezones_df = self.spark.read.json(str(TIMEZONE_DEFINITION_DATASET))
        registrations_df = self.spark.read.parquet(
            str(REGISTRATION_DATASET), schema=registration_schema
        ).select("country", "user_id", "registration_timestamp")

        registrations_with_local_timezones_df = self.joiner.join_registrations_with_local_timezones(
            registrations_df, timezones_df
        )

        sessions_registrations_joined_df = sessions_df.join(
            other=registrations_with_local_timezones_df, how="left", on="user_id"
        )

        session_stats_df = self.calculate_user_session_stats(
            sessions_registrations_joined_df, window_user_spec
        )

        return session_stats_df

    def calculate_match_stats(self) -> DataFrame:
        matches_df = self.spark.read.parquet(str(MATCH_DATASET), schema = match_schema)

        match_stats_df = matches_df \
            .transform(self.joiner.pair_matches) \
            .transform(self.calculate_match_time) \
            .transform(self.calculate_user_match_points)

        return match_stats_df

    def calculate_user_session_stats(
        self, session_df: DataFrame, window_user_spec: Window
    ) -> DataFrame:
        user_session_stats_df = (
            session_df.withColumn(
                "previous_session_timestamp",
                lag("session_timestamp").over(window_user_spec),
            )
            .withColumn(
                "time_diff",
                unix_timestamp(col("session_timestamp"))
                - unix_timestamp(col("previous_session_timestamp")),
            )
            .withColumn(
                "session_duration",
                sum("time_diff").over(window_user_spec)
            )
            .withColumn(
                "is_new_session",
                when(col("time_diff") <= self.ping_treshold_sec, False).otherwise(True),
            )
            .orderBy("session_timestamp")
        )

        return user_session_stats_df

    def calculate_match_time(self, matches_df: DataFrame) -> DataFrame:
        matches_df_with_match_duration = matches_df \
            .withColumn(
                "match_duration",
                unix_timestamp(col("end_time")) - unix_timestamp(col("start_time")),
            )

        return matches_df_with_match_duration

    def calculate_user_match_points(self, match_stats_df: DataFrame) -> DataFrame:
        match_stats_with_user_points_df = match_stats_df.withColumn(
            "home_user_points",
            when(col("home_goals_scored") > col("away_goals_scored"), 3)
            .when(col("home_goals_scored") < col("away_goals_scored"), 0)
            .when(col("home_goals_scored") == col("away_goals_scored"), 1),
        ).withColumn(
            "away_user_points",
            when(col("away_goals_scored") > col("home_goals_scored"), 3)
            .when(col("away_goals_scored") < col("home_goals_scored"), 0)
            .when(col("away_goals_scored") == col("home_goals_scored"), 1),
        )

        return match_stats_with_user_points_df
