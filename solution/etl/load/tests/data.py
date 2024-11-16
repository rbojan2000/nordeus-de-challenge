from pyspark.sql import SparkSession, DataFrame
from load.schema import match_schema, match_stats_schema
from datetime import datetime

class DataGenerator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_matches_test_data(self) -> DataFrame:
        data = [
            {
                "event_id": 402,
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": None,
                "away_goals_scored": None,
                "match_timestamp": datetime.fromtimestamp(1728390690),
                "match_status": "match_start",
            },
            {
                "event_id": 406,
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": 5,
                "away_goals_scored": 0,
                "match_timestamp": datetime.fromtimestamp(1728390791),
                "match_status": "match_end",
            }
        ]
        return self.spark.createDataFrame(data = data, schema=match_schema)

    def get_paired_match_test_data(self) -> DataFrame:
        data = [
            {
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": 5,
                "away_goals_scored": 0,
                "start_time": datetime.fromtimestamp(1728390690),
                "end_time": datetime.fromtimestamp(1728390791),
                "match_duration": None,
                "home_user_points": None,
                "away_user_points": None,
            }
        ]

        return self.spark.createDataFrame(data = data, schema=match_stats_schema)
