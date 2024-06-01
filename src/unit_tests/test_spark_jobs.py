from chispa.dataframe_comparer import *
from ..jobs.job_1 import job_1
from collections import namedtuple
from ..jobs.job_2 import job_2
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

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


def test_job_2(spark_session):
    current_date = "2024-03-28"
    cumulated_table_name: str = "web_users_cumulated"
    event_table_name: str = "web_events"
    
    cumulative_schema = StructType([
        StructField("user_id", LongType(), nullable=True),
        StructField("dates_active", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("date", StringType(), nullable=False)
    ])
    
    input_cumulative_data = [
        InputCumulative(1, ["2024-03-27"], "2024-03-27"),
        InputCumulative(3, ["2024-03-27"], "2024-03-27"),
        InputCumulative(4, [None, None, "2024-03-25"], "2024-03-27")
    ]
    cumulated_df = spark_session.createDataFrame(input_cumulative_data, cumulative_schema)
    
    input_event_data = [
        InputEvents(1, "2024-03-28 08:00:00"),
        InputEvents(2, "2024-03-28 09:10:30"),
        InputEvents(4, "2024-03-28 10:30:00"),
        InputEvents(4, "2024-03-28 11:30:00")
    ]
    event_df = spark_session.createDataFrame(input_event_data)
    print(event_df)

    actual_df = job_2(spark_session, cumulated_df, cumulated_table_name, event_df, event_table_name, current_date)
    
    expected_data = [
        OutputCumulative(1, ["2024-03-28", "2024-03-27"], "2024-03-28"),        
        OutputCumulative(2, ["2024-03-28"], "2024-03-28"),
        OutputCumulative(3, [None, "2024-03-27"], "2024-03-28"),
        OutputCumulative(4, ["2024-03-28", None, None, "2024-03-25"], "2024-03-28")
    ]
    expected_df = spark_session.createDataFrame(expected_data, cumulative_schema)
    
    assert_df_equality(actual_df.sort("user_id", "date"), expected_df.sort("user_id", "date"))
