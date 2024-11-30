from load.constants import PING_TRESHOLD_SEC
from load.joiner import Joiner
from load.paths import (
    MATCH_DATASET,
    REGISTRATION_DATASET,
    SESSION_DATASET,
    TIMEZONE_DEFINITION_DATASET,
)
from load.schema import match_schema, registration_schema, session_schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lag, sum, unix_timestamp, when
from pyspark.sql.window import Window


class Analyzer:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.joiner = Joiner()
        self.ping_treshold_sec = PING_TRESHOLD_SEC
        self.window_user_spec = Window.partitionBy("user_id").orderBy(
            "session_timestamp"
        )

    def calculate_session_stats(self) -> DataFrame:
        sessions_df = self.spark.read.parquet(
            str(SESSION_DATASET), schema=session_schema
        )
        timezones_df = self.spark.read.json(str(TIMEZONE_DEFINITION_DATASET))
        registrations_df = self.spark.read.parquet(
            str(REGISTRATION_DATASET), schema=registration_schema
        ).select("country", "user_id", "registration_timestamp")

        registrations_with_local_timezones_df = (
            self.joiner.join_registrations_with_local_timezones(
                registrations_df, timezones_df
            )
        )
        sessions_registrations_joined_df = self.joiner.join_sessions_with_registrations(
            sessions_df, registrations_with_local_timezones_df
        )

        user_session_stats_df = (
            sessions_registrations_joined_df
            .transform(self.assign_previous_session_ping_timestamp)
            .transform(self.calculate_time_time_diff_session_pings)
            .transform(self.calculate_session_duration)
            .transform(self.determine_new_session)
            .orderBy("session_timestamp")
        )

        return user_session_stats_df

    def calculate_match_stats(self) -> DataFrame:
        matches_df = self.spark.read.parquet(str(MATCH_DATASET), schema=match_schema)

        match_stats_df = (
            matches_df.transform(self.joiner.pair_matches)
            .transform(self.calculate_match_time)
            .transform(self.calculate_user_match_points)
        )

        return match_stats_df

    def assign_previous_session_ping_timestamp(
        self, sessions_df: DataFrame
    ) -> DataFrame:
        return sessions_df.withColumn(
            "previous_session_timestamp",
            lag("session_timestamp").over(self.window_user_spec),
        )

    def calculate_time_time_diff_session_pings(
        self, sessions_df: DataFrame
    ) -> DataFrame:
        return sessions_df.withColumn(
            "time_diff",
            unix_timestamp(col("session_timestamp"))
            - unix_timestamp(col("previous_session_timestamp")),
        )

    def calculate_session_duration(self, sessions_df: DataFrame):
        return sessions_df.withColumn(
            "session_duration", sum("time_diff").over(self.window_user_spec)
        )

    def determine_new_session(self, sessions_df: DataFrame):
        return sessions_df.withColumn(
            "is_new_session",
            when(col("time_diff") <= self.ping_treshold_sec, False).otherwise(True),
        )

    def calculate_match_time(self, matches_df: DataFrame) -> DataFrame:
        return matches_df.withColumn(
            "match_duration",
            unix_timestamp(col("end_time")) - unix_timestamp(col("start_time")),
        )

    def calculate_user_match_points(self, match_stats_df: DataFrame) -> DataFrame:
        return match_stats_df.withColumn(
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
