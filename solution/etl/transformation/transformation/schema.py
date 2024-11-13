from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

registration_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", LongType(), nullable=False),
        StructField(
            "event_data",
            StructType(
                [
                    StructField("country", StringType(), nullable=False),
                    StructField("user_id", StringType(), nullable=False),
                    StructField("device_os", StringType(), nullable=False),
                ]
            ),
        ),
    ]
)

session_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", LongType(), nullable=False),
        StructField(
            "event_data",
            StructType(
                [
                    StructField("user_id", StringType(), nullable=False),
                    StructField("type", StringType(), nullable=False),
                ]
            ),
        ),
    ]
)

match_schema = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", LongType(), nullable=False),
        StructField("match_status", StringType(), nullable=False),
        StructField(
            "event_data",
            StructType(
                [
                    StructField("match_id", StringType(), nullable=False),
                    StructField("home_user_id", StringType(), nullable=False),
                    StructField("away_user_id", StringType(), nullable=False),
                    StructField("home_goals_scored", IntegerType(), nullable=True),
                    StructField("away_goals_scored", IntegerType(), nullable=True),
                ]
            ),
        ),
    ]
)
