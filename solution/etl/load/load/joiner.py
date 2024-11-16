from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_utc_timestamp

class Joiner():
    def __init__(self):
        pass
            
    def join_registrations_with_local_timezones(
        self, registrations_df: DataFrame, timezones_df: DataFrame
    ) -> DataFrame:
        registrations_with_local_timezones_df = registrations_df.join(
            other=timezones_df, how="left", on="country"
        ).withColumn(
            "registration_local_datetime",
            from_utc_timestamp(timestamp=col("registration_timestamp"), tz=col("timezone")),
        )
        
        return registrations_with_local_timezones_df
    
    def pair_matches(self, matches_df: DataFrame) -> DataFrame:
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
        paired_matches = match_start_df.join(match_end_df, "match_id")
        
        return paired_matches
