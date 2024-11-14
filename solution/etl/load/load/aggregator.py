from load.paths import (
    MATCH_DATASET,
    REGISTRATION_DATASET,
    SESSION_DATASET,
    TIMEZONE_DEFINITION_DATASET,
)
from load.constants import PING_TRESHOLD_SEC
from load.schema import match_schema, registration_schema, session_schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, lag, unix_timestamp, when
from pyspark.sql.window import Window


class Aggregator:
    
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.ping_treshold_sec = PING_TRESHOLD_SEC
        
    def calculate_session_stats(self) -> DataFrame:
        window_user_spec = Window.partitionBy("user_id").orderBy("session_timestamp")

        sessions_df = self.spark.read.parquet(str(SESSION_DATASET), schema=session_schema)
        timezones_df = self.spark.read.json(str(TIMEZONE_DEFINITION_DATASET))
        registrations_df = self.spark.read.parquet(
            str(REGISTRATION_DATASET), schema=registration_schema
        ).select("country", "user_id", "registration_timestamp")

        registrations_with_local_timezones_df = self.join_registrations_with_local_timezones(
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
                "is_new_session",
                when(col("time_diff") < self.ping_treshold_sec, False).otherwise(True),
            )
        )

        return user_session_stats_df

    def join_registrations_with_local_timezones(
        self, registrations_df: DataFrame, timezones_df: DataFrame
    ) -> DataFrame:
        registrations_with_local_timezones_df = registrations_df.join(
            other=timezones_df, how="left", on="country"
        ).withColumn(
            "local_datetime",
            from_utc_timestamp(timestamp=col("registration_timestamp"), tz=col("timezone")),
        )

        return registrations_with_local_timezones_df

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

    def calculate_match_time(self, matches_df: DataFrame) -> DataFrame:
        match_start_df = matches_df.filter(col("match_status") == "match_start").select(
            "match_id",
            "home_user_id",
            "away_user_id",
            col("match_timestamp").alias("start_time"),
        )

        match_end_df = matches_df.filter(col("match_status") == "match_end").select(
            "match_id",
            "home_goals_scored",
            "away_goals_scored",
            col("match_timestamp").alias("end_time"),
        )

        paired_matches = match_start_df.join(match_end_df, "match_id").withColumn(
            "match_duration",
            unix_timestamp(col("end_time")) - unix_timestamp(col("start_time")),
        )

        return paired_matches
