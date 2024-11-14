from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

registration_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("registration_timestamp", TimestampType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("device_os", StringType(), nullable=False),
    ]
)

session_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("session_timestamp", TimestampType(), nullable=False),
        StructField("type", StringType(), nullable=False),
    ]
)

match_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("match_id", StringType(), nullable=False),
        StructField("home_user_id", StringType(), nullable=False),
        StructField("away_user_id", StringType(), nullable=False),
        StructField("home_goals_scored", IntegerType(), nullable=True),
        StructField("away_goals_scored", IntegerType(), nullable=True),
        StructField("match_timestamp", TimestampType(), nullable=False),
        StructField("match_status", StringType(), nullable=False),
    ]
)
