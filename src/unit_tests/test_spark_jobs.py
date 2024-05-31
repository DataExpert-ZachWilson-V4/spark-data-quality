from chispa.dataframe_comparer import *
import pytest
from pyspark.sql import SparkSession
from ..jobs.job_1 import job_1
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)
from collections import namedtuple


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


Game = namedtuple(
    "Game",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)


def test_game_details_dedupe(spark):
    input_data = [
        Game(
            20801112,
            1610612758,
            "SAC",
            "Sacramento",
            201150,
            "Spencer Hawes",
            None,
            "C",
            None,
            "35:41",
            6.0,
            13.0,
            0.462,
            1.0,
            3.0,
            0.333,
            4.0,
            4.0,
            1.0,
            2.0,
            4.0,
            6.0,
            5.0,
            0.0,
            2.0,
            1.0,
            2.0,
            17.0,
            -14.0,
        ),
        Game(
            20801112,
            1610612758,
            "SAC",
            "Sacramento",
            201150,
            "Spencer Hawes",
            None,
            "C",
            None,
            "35:41",
            6.0,
            13.0,
            0.462,
            1.0,
            3.0,
            0.333,
            4.0,
            4.0,
            1.0,
            2.0,
            4.0,
            6.0,
            5.0,
            0.0,
            2.0,
            1.0,
            2.0,
            17.0,
            -14.0,
        ),
    ]

    schema = StructType(
        [
            StructField("game_id", LongType(), True),
            StructField("team_id", LongType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_city", StringType(), True),
            StructField("player_id", LongType(), True),
            StructField("player_name", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("min", StringType(), True),
            StructField("fgm", DoubleType(), True),
            StructField("fga", DoubleType(), True),
            StructField("fg_pct", DoubleType(), True),
            StructField("fg3m", DoubleType(), True),
            StructField("fg3a", DoubleType(), True),
            StructField("fg3_pct", DoubleType(), True),
            StructField("ftm", DoubleType(), True),
            StructField("fta", DoubleType(), True),
            StructField("ft_pct", DoubleType(), True),
            StructField("oreb", DoubleType(), True),
            StructField("dreb", DoubleType(), True),
            StructField("reb", DoubleType(), True),
            StructField("ast", DoubleType(), True),
            StructField("stl", DoubleType(), True),
            StructField("blk", DoubleType(), True),
            StructField("to", DoubleType(), True),
            StructField("pf", DoubleType(), True),
            StructField("pts", DoubleType(), True),
            StructField("plus_minus", DoubleType(), True),
        ]
    )

    input_dataframe = spark.createDataFrame(input_data, schema=schema)

    actual_df = job_1(spark, "nba_game_details", input_dataframe)
    expected_output = [
        Game(
            20801112,
            1610612758,
            "SAC",
            "Sacramento",
            201150,
            "Spencer Hawes",
            None,
            "C",
            None,
            "35:41",
            6.0,
            13.0,
            0.462,
            1.0,
            3.0,
            0.333,
            4.0,
            4.0,
            1.0,
            2.0,
            4.0,
            6.0,
            5.0,
            0.0,
            2.0,
            1.0,
            2.0,
            17.0,
            -14.0,
        )
    ]

    expected_df = spark.createDataFrame(expected_output, schema=schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
