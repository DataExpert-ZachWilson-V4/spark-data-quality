from chispa.dataframe_comparer import *
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    DateType,
    StringType,
    DoubleType,
    LongType,
)
from collections import namedtuple
import shutil
import os

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()


Game_Details = namedtuple(
    "Game",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position",
)

def test_job_1(spark):
    input_data = [
        Game_Details(
            1,
            1,
            "SEA",
            "Seattle",
            123,
            "Test Name",
            None,
            "D",
        ),
        Game_Details(
            1,
            1,
            "SEA",
            "Seattle",
            123,
            "Test Name",
            None,
            "D",
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
        ]
    )

    input_df = spark.createDataFrame(input_data, schema=schema)
    input_df.createOrReplaceTempView("nba_game_details")

    actual_df = job_1(spark, "nba_game_details", "nba_game_dedupe")
    expected_output = [
        Game_Details(
            1,
            1,
            "SEA",
            "Seattle",
            123,
            "Test Name",
            None,
            "D",
        )
    ]

    expected_df = spark.createDataFrame(expected_output, schema=schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job_2(spark):

    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")
#     films = namedtuple("films", "year film votes rating film_id")

    actor_films_input = [
        actor_films(
        "Sally Field",
        "nm0000398",
        "National Theatre Live: All My Sons",
        "2019",
        "137",
        "8.6",
        "tt9382224",

    )]
    actor_films_df = spark.createDataFrame(actor_films_input)
    actor_films_df.createOrReplaceTempView("actor_films")

    actors_input = [(
        actors(
                "Sally Field",
                "nm0000398",
                [["2019", "National Theatre Live: All My Sons", "137", "8.6", "tt9382224"]],
                "star",
                True,
                "2019"
        )
    )]

    actors_input_df = spark.createDataFrame(actors_input)
    actors_input_df.createOrReplaceTempView("actors")

    actual_df = job_2(spark, "actors", "actor_films")

    expected_output = [
        actors(
                "Sally Field",
                "nm0000398",
                [["2019", "National Theatre Live: All My Sons", "137", "8.6", "tt9382224"]],
                "star",
                True,
                "2019"
        )
    ]

    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)