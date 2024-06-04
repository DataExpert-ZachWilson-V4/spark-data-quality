from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import date, datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    FloatType,
    BooleanType,
)

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

NBAGameDetails = namedtuple(
    "NBAGameDetails",
    "game_id team_id player_id player_name"
)

def test_job_1(spark_session):
    # Create a DataFrame with duplicate data for testing

    nba_game_details = [
        NBAGameDetails("21000424","1610612755","201150","Spencer Hawes"),
        NBAGameDetails("21000555","1610612755","201150","Spencer Hawes"),
        NBAGameDetails("21000424","1610612755","201150","Spencer Hawes"),
        NBAGameDetails("21000555","1610612755","201150","Spencer Hawes"),
    ]
    nba_game_details_deduped = [
        NBAGameDetails("21000424","1610612755","201150","Spencer Hawes"),
        NBAGameDetails("21000555","1610612755","201150","Spencer Hawes"),
    ]

    schema = StructType(
        [
            StructField("game_id", StringType(), True),
            StructField("team_id", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("player_name", StringType(), True),
        ]
    )
    nba_game_details_df = spark_session("job_1").createDataFrame(
        nba_game_details, schema
    )
    nba_game_details_deduped_df = spark_session("job_1").createDataFrame(
        nba_game_details_deduped, schema
    )
    query_output = job_1(
        spark_session("job_1"), "nba_game_details", nba_game_details_df
    )
    assert_df_equality(query_output, nba_game_details_deduped_df)
    

PlayerSeason = namedtuple("PlayerSeason", "player_name is_active current_season")
PlayerScd = namedtuple("PlayerScd", "player_name is_active start_season end_season")


def test_job_2(spark_session):
    input_table_name: str = "nba_players"
    source_data = [
        PlayerSeason("Michael Jordan", True, 2001),
        PlayerSeason("Michael Jordan", True, 2002),
        PlayerSeason("Michael Jordan", True, 2003),
        PlayerSeason("Michael Jordan", False, 2004),
        PlayerSeason("Michael Jordan", False, 2005),
        PlayerSeason("Scottie Pippen", True, 2001),
        PlayerSeason("Scottie Pippen", False, 2002),
        PlayerSeason("Scottie Pippen", False, 2003),
        PlayerSeason("Scottie Pippen", True, 2004),
        PlayerSeason("Scottie Pippen", True, 2005),
        PlayerSeason("LeBron James", True, 2003),
        PlayerSeason("LeBron James", True, 2004),
        PlayerSeason("LeBron James", True, 2005)
    ]
    source_df = spark_session.createDataFrame(source_data)

    from jobs.job_2 import job_2
    actual_df = job_2(spark_session, source_df, input_table_name)
    expected_data = [
        PlayerScd("Michael Jordan", True, 2001, 2003),
        PlayerScd("Michael Jordan", False, 2004, 2005),
        PlayerScd("Scottie Pippen", True, 2001, 2001),
        PlayerScd("Scottie Pippen", False, 2002, 2003),
        PlayerScd("Scottie Pippen", True, 2004, 2005),
        PlayerScd("LeBron James", True, 2003, 2005)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df.sort("player_name", "start_season"), expected_df.sort("player_name", "start_season"))
