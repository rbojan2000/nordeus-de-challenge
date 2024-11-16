import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark_test import assert_pyspark_df_equal
from datetime import datetime
from load.analyzer import Analyzer
from load.schema import match_stats_schema
from tests.data import DataGenerator

class TestAnalyzer:
    @pytest.fixture(scope="session")
    def spark(self) -> SparkSession:
        return SparkSession.builder.appName('load_test_session').getOrCreate()

    @pytest.fixture(scope="session")
    def analyzer(self, spark) -> Analyzer:
        return Analyzer(spark)

    @pytest.fixture(scope="session")
    def test_data(self, spark) -> DataGenerator:
        return DataGenerator(spark)

    def test_calculate_user_match_points(self, spark, analyzer, test_data) -> None:
        paired_match_df: DataFrame = test_data.get_paired_match_test_data()
        
        expected_data = [
            {
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": 5,
                "away_goals_scored": 0,
                "start_time": datetime.fromtimestamp(1728390690),
                "end_time": datetime.fromtimestamp(1728390791),
                "match_duration": None,
                "home_user_points": 3,
                "away_user_points": 0,
            }
        ]

        result_df: DataFrame = analyzer.calculate_user_match_points(paired_match_df)
        expected_df = spark.createDataFrame(data=expected_data, schema=match_stats_schema)

        assert_pyspark_df_equal(result_df, expected_df)

    def test_calculate_match_duration(self, spark, analyzer, test_data) -> None:
        paired_match_df: DataFrame = test_data.get_paired_match_test_data()
        
        expected_data = [
            {
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": 5,
                "away_goals_scored": 0,
                "start_time": datetime.fromtimestamp(1728390690),
                "end_time": datetime.fromtimestamp(1728390791),
                "match_duration": 1728390791-1728390690,
                "home_user_points": None,
                "away_user_points": None,
            }
        ]
        result_df: DataFrame = analyzer.calculate_match_time(paired_match_df)

        expected_df = spark.createDataFrame(data=expected_data, schema=match_stats_schema)
        assert_pyspark_df_equal(result_df, expected_df)
