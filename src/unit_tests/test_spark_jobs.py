from chispa.dataframe_comparer import *
from collections import namedtuple

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
import pytest
from pyspark.sql import SparkSession
from typing import Callable
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    FloatType,
    BooleanType,
)

ActorFilms = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
Actors = namedtuple(
    "Actors", "actor actor_id films quality_class is_active current_year"
)
NBAGameDetails = namedtuple(
    "NBAGameDetails",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)


def test_job1(spark_session) -> None:
    spark = spark_session("job_1")
    # Setup the films
    actor_films_1914 = [
        ActorFilms(
            "Charles Chaplin",
            "nm0000122",
            "Tillie's Punctured Romance",
            1914,
            3301,
            6.3,
            "tt0004707",
        ),
        ActorFilms(
            "Lillian Gish",
            "nm0001273",
            "Judith of Bethulia",
            1914,
            1259,
            6.1,
            "tt0004181",
        ),
        ActorFilms(
            "Lillian Gish", "nm0001273", "Home, Sweet Home", 1914, 190, 5.8, "tt0003167"
        ),
    ]
    # Get the cumulative insertion query output
    actors_1914 = [
        Actors(
            "Lillian Gish",
            "nm0001273",
            [
                ["Judith of Bethulia", 1259, 6.1, "tt0004181", 1914],
                ["Home, Sweet Home", 190, 5.8, "tt0003167", 1914],
            ],
            "bad",
            True,
            1914,
        ),
        Actors(
            "Charles Chaplin",
            "nm0000122",
            [["Tillie's Punctured Romance", 3301, 6.3, "tt0004707", 1914]],
            "average",
            True,
            1914,
        ),
    ]
    # Setup input and output schemas
    input_schema = StructType(
        [
            StructField("actor", StringType(), True),
            StructField("actor_id", StringType(), True),
            StructField("film", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("film_id", StringType(), True),
        ]
    )
    films_schema = StructType(
        [
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("film_id", StringType(), True),
            StructField("year", IntegerType(), True),
        ]
    )
    output_schema = StructType(
        [
            StructField("actor", StringType(), True),
            StructField("actor_id", StringType(), True),
            StructField("films", ArrayType(films_schema), True),
            StructField("quality_class", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("current_year", IntegerType(), True),
        ]
    )
    actor_films_1914_df = spark.createDataFrame(actor_films_1914, schema=input_schema)
    # Since this Query is a cumulative insertion query we need to create
    # the output table schema as a view before running the query to avoid
    # the error of the table not existing
    actors_1914_df = spark.createDataFrame(actors_1914, schema=output_schema)
    actors_1914_df.createOrReplaceTempView("actors")

    # Run query
    query_1914_output = job_1(
        spark_session=spark,
        dataframe=actor_films_1914_df,
        output_table_name="actors",
        input_table_name="actor_films",
        last_year=1913,
    )
    # Check the output of the query
    assert_df_equality(query_1914_output, actors_1914_df)


def test_job2(spark_session) -> None:
    # Setup the NBA Game Details
    nba_game_details = [
        NBAGameDetails(
            "21000424",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "19:43",
            3,
            8,
            0.375,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            3,
            4,
            7,
            1,
            0,
            0,
            4,
            2,
            6,
            -7,
        ),
        NBAGameDetails(
            "21000555",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "4:56",
            0,
            2,
            0.0,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            0,
            2,
            2,
            0,
            0,
            0,
            1,
            0,
            0,
            -11,
        ),
        NBAGameDetails(
            "21000424",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "19:43",
            3,
            8,
            0.375,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            3,
            4,
            7,
            1,
            0,
            0,
            4,
            2,
            6,
            -7,
        ),
        NBAGameDetails(
            "21000555",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "4:56",
            0,
            2,
            0.0,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            0,
            2,
            2,
            0,
            0,
            0,
            1,
            0,
            0,
            -11,
        ),
    ]
    # Get the deduped NBA Game Details
    nba_game_details_deduped = [
        NBAGameDetails(
            "21000424",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "19:43",
            3,
            8,
            0.375,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            3,
            4,
            7,
            1,
            0,
            0,
            4,
            2,
            6,
            -7,
        ),
        NBAGameDetails(
            "21000555",
            "1610612755",
            "PHI",
            "Philadelphia",
            "201150",
            "Spencer Hawes",
            None,
            "C",
            None,
            "4:56",
            0,
            2,
            0.0,
            0,
            0,
            0.0,
            0,
            0,
            0.0,
            0,
            2,
            2,
            0,
            0,
            0,
            1,
            0,
            0,
            -11,
        ),
    ]
    # Setup input and output schemas for the NBA Game Details
    schema = StructType(
        [
            StructField("game_id", StringType(), True),
            StructField("team_id", StringType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_city", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("min", StringType(), True),
            StructField("fgm", IntegerType(), True),
            StructField("fga", IntegerType(), True),
            StructField("fg_pct", FloatType(), True),
            StructField("fg3m", IntegerType(), True),
            StructField("fg3a", IntegerType(), True),
            StructField("fg3_pct", FloatType(), True),
            StructField("ftm", IntegerType(), True),
            StructField("fta", IntegerType(), True),
            StructField("ft_pct", FloatType(), True),
            StructField("oreb", IntegerType(), True),
            StructField("dreb", IntegerType(), True),
            StructField("reb", IntegerType(), True),
            StructField("ast", IntegerType(), True),
            StructField("stl", IntegerType(), True),
            StructField("blk", IntegerType(), True),
            StructField("to", IntegerType(), True),
            StructField("pf", IntegerType(), True),
            StructField("pts", IntegerType(), True),
            StructField("plus_minus", IntegerType(), True),
        ]
    )
    nba_game_details_df = spark_session("job_2").createDataFrame(
        nba_game_details, schema
    )
    nba_game_details_deduped_df = spark_session("job_2").createDataFrame(
        nba_game_details_deduped, schema
    )
    # Run the query
    query_output = job_2(
        spark_session=spark_session("job_2"),
        dataframe=nba_game_details_df,
        input_table_name="nba_game_details",
    )
    # Check the output of the query
    assert_df_equality(query_output, nba_game_details_deduped_df)
