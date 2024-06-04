from chispa.dataframe_comparer import *
from src.jobs.job_1 import job_1
from collections import namedtuple
from src.jobs.job_2 import job_2
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DateType
import pytest

Input_nba_game_details = namedtuple("Input_nba_game_details", "game_id team_id player_id m_field_goals_made dim_team_did_win")
Output_nba_game_details = namedtuple("Output_nba_game_details", "game_id team_id player_id m_field_goals_made dim_team_did_win")

def test_job_1(spark_session):
    input_table_name: str = "fct_nba_game_details"
    source_data = [
        Input_nba_game_details(41100403, 1610612748, 2547, 3, True),
        Input_nba_game_details(41100404, 1610612749, 2548, 4, False)
    ]
    source_df = spark_session.createDataFrame(source_data)
    source_df.createOrReplaceTempView("fct_nba_game_details")

    actual_df = job_1(spark_session, input_table_name)
    expected_data = [
        Output_nba_game_details(41100403, 1610612748, 2547, 3, True),
        Output_nba_game_details(41100404, 1610612749, 2548, 4, False)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)


Input_Cumulative = namedtuple("Input_Cumulative", "host host_activity_datelist date")
Base_Events = namedtuple("Base_Events", "user_id device_id host url event_time")
Output_Cumulative = namedtuple("Output_Cumulative", "host host_activity_datelist date")   

def test_job_2(spark_session):

    cumulated_table_name: str = "hosts_cumulated"
    event_table_name: str = "web_events"
    current_date: str = "2024-03-28"

    cumulative_schema = StructType([
        StructField("host", StringType(), nullable=True),
        StructField("host_activity_datelist", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("date", StringType(), nullable=True)
    ])

    Input_Cumulative_data = [
        Input_Cumulative("www.eczachly.com", ["2024-03-27"], "2024-03-27 23:57:37"),
        Input_Cumulative("www.aayushi.com", ["2024-03-26", "2024-03-27"], "2024-03-27 23:57:37")
    ]
    cumulated_df = spark_session.createDataFrame(Input_Cumulative_data, cumulative_schema)
    cumulated_df.createOrReplaceTempView("hosts_cumulated")

    Base_Event_schema = StructType([
        StructField("user_id", LongType(), nullable=True),
        StructField("device_id", LongType(), nullable=True),
        StructField("host", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True)
    ])

    Base_Event_data = [
        Base_Events(694175222, 1847648591, "aayushi", "/", "2024-03-28 23:57:37"),
        Base_Events(694175223, 1847648592, "aayushi", "/", "2024-03-29 23:57:37")
    ]
    event_df = spark_session.createDataFrame(Base_Event_data, Base_Event_schema)
    event_df.createOrReplaceTempView("web_events")

    actual_df = job_2(spark_session, cumulated_table_name, event_table_name, current_date)

    expected_data = [
        Output_Cumulative("www.eczachly.com", ["2024-03-26", "2024-03-27"], "2024-03-28 23:57:37"),        
        Output_Cumulative("www.aayushi.com", ["2024-03-28"], "2024-03-28 23:57:37")
    ]
    expected_df = spark_session.createDataFrame(expected_data, cumulative_schema)

    assert_df_equality(actual_df.sort("host", "host_activity_datelist"), expected_df.sort("host", "host_activity_datelist"))