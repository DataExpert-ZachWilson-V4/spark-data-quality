from chispa.dataframe_comparer import *
from collections import namedtuple
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

GameDetails = namedtuple(
    "GameDetails",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg3m fg3a ftm fta oreb dreb reb ast stl blk to pf pts plus_minus",
)

WebEvents = namedtuple(
    "WebEvents", "host event_time"
)
HostCumulated = namedtuple(
    "HostCumulated", "host host_activity_datelist date"
)

def test_job_1_deduplication(spark_session: SparkSession):
    # Provide 2 records that have the same game_id, team_id, player_id to make sure that it gets deduped
    # Provide 1 record that doesn't have a duplicate to ensure it stays in the dataframe as expected
    source_data = [
        GameDetails("12345678", "1111111111", 'SUM', 'Summit', 555555, 'Hilary', None, 'C', None, '01:59', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        GameDetails("12345678", "1111111111", 'DEN', 'Denver', 555555, 'Hilary', 'Dup', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
        GameDetails("20900108", "1610612758", 'SAC', 'Sacramento', 201150, 'Spencer Hawes', None, 'C', None, '31:59', 5, 10, 0, 3, 0, 2, 2, 2, 6, 8, 2, 4, 1, 4, 12, -5)
    ]
    expected_schema = StructType(
        [
            StructField("game_id", StringType(), False),
            StructField("team_id", StringType(), False),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_city", StringType(), False),
            StructField("player_id", StringType(), False),
            StructField("player_name", StringType(), False),
            StructField("nickname", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("min", StringType(), True),
            StructField("fgm", LongType(), True),
            StructField("fga", LongType(), True),
            StructField("fg3m", LongType(), True),
            StructField("fg3a", LongType(), True),
            StructField("ftm", LongType(), True),
            StructField("fta", LongType(), True),
            StructField("oreb", LongType(), True),
            StructField("dreb", LongType(), True),
            StructField("reb", LongType(), True),
            StructField("ast", LongType(), True),
            StructField("stl", LongType(), True),
            StructField("blk", LongType(), True),
            StructField("to", LongType(), True),
            StructField("pf", LongType(), True),
            StructField("pts", LongType(), True),
            StructField("plus_minus", LongType(), True),
        ]
    )

    source_df = spark_session.createDataFrame(source_data, schema=expected_schema)
    result_df = job_1(spark_session, source_df)
    expected_data = [
        GameDetails("12345678", "1111111111", 'SUM', 'Summit', 555555, 'Hilary', None, 'C', None, '01:59', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        GameDetails("20900108", "1610612758", 'SAC', 'Sacramento', 201150, 'Spencer Hawes', None, 'C', None, '31:59', 5, 10, 0, 3, 0, 2, 2, 2, 6, 8, 2, 4, 1, 4, 12, -5)
    ]
    expected_df = spark_session.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(result_df, expected_df)

def test_job_2_hosts_cumulated(spark_session: SparkSession):
    start_date = '2021-01-03'
    output_table_name = "cumulative_host"

    # Have one case where in past cumulative host data and not in start_date + 1 web_events data
    # Have one case where in not past cumulative host data (new) and in start_date + 1 web_events data
    # Have one case where in past cumulative host data and in start_date + 1 web_events data
    new_data = [
        WebEvents("www.abc.com", datetime.datetime(2021, 1, 4, 5, 6, 7, 8)),
        WebEvents("www.new_site.com", datetime.datetime(2021, 1, 4, 6, 18, 49, 12))
    ]
    web_events_df = spark_session.createDataFrame(new_data)

    prev_cumulative_host_data = [
        HostCumulated("www.abc.com", [datetime.datetime(2021, 1, 2), datetime.datetime(2021, 1, 1)], datetime.datetime(2021, 1, 3)),
        HostCumulated("www.def.com", [datetime.datetime(2021, 1, 3)], datetime.datetime(2021, 1, 3))
    ]
    prev_cumulative_host_df = spark_session.createDataFrame(prev_cumulative_host_data)
    
    result_df = job_2(spark_session, web_events_df, prev_cumulative_host_df, output_table_name, start_date)
    expected_data = [
        HostCumulated("www.abc.com", [datetime.datetime(2021, 1, 4), datetime.datetime(2021, 1, 2), datetime.datetime(2021, 1, 1)], datetime.datetime(2021, 1, 4)),
        HostCumulated("www.def.com", [datetime.datetime(2021, 1, 3)], datetime.datetime(2021, 1, 4)),
        HostCumulated("www.new_site.com", [datetime.datetime(2021, 1, 4)], datetime.datetime(2021, 1, 4))
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(result_df, expected_df)