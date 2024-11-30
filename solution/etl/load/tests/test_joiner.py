from datetime import datetime

import pytest
from load.joiner import Joiner
from load.schema import paired_matches_schema
from pyspark.sql import DataFrame, SparkSession
from pyspark_test import assert_pyspark_df_equal
from tests.data import DataGenerator


class TestJoiner:
    @pytest.fixture(scope="session")
    def spark(self) -> SparkSession:
        return SparkSession.builder.appName("load_test_session").getOrCreate()

    @pytest.fixture(scope="session")
    def joiner(self) -> Joiner:
        return Joiner()

    @pytest.fixture(scope="session")
    def test_data(self, spark) -> DataGenerator:
        return DataGenerator(spark)

    def test_pair_matches(self, spark, joiner, test_data) -> None:
        matches_df: DataFrame = test_data.get_matches_test_data()

        expected_data = [
            {
                "match_id": "290d5196-9123-11ef-b02f-bafd2d38177c",
                "home_user_id": "d7f20e07-ed42-02ed-c4bb-895c608099f6",
                "away_user_id": "5d357ffe-4423-f60d-db0e-da407f5e8e61",
                "home_goals_scored": 5,
                "away_goals_scored": 0,
                "start_time": datetime.fromtimestamp(1728390690),
                "end_time": datetime.fromtimestamp(1728390791),
            }
        ]

        result_df: DataFrame = joiner.pair_matches(matches_df)
        result_df.printSchema()
        result_df.show()

        expected_df = spark.createDataFrame(
            data=expected_data, schema=paired_matches_schema
        )
        expected_df.printSchema()

        assert_pyspark_df_equal(result_df, expected_df)
