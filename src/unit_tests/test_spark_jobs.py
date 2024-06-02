from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    DateType,
    BooleanType
)
from collections import namedtuple
from chispa.dataframe_comparer import *

from collections import namedtuple

from jobs import job_2, job_1

PlayerSeason = namedtuple("PlayerSeason", "player_name is_active current_season")
PlayerScd = namedtuple("PlayerScd", "player_name is_active start_season end_season")


def test_job_1(spark_session):
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

    from jobs.job_1 import job_1
    actual_df = job_1(spark_session, source_df, input_table_name)
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


InputCumulative = namedtuple("InputCumulative", "user_id dates_active date")
InputEvents = namedtuple("InputEvents", "user_id event_time")
OutputCumulative = namedtuple("OutputCumulative", "user_id dates_active date")


