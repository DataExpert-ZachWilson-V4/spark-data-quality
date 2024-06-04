import sys
import os
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date
from pyspark.sql.types import (StructType, StructField, ArrayType, StringType, DoubleType, LongType, BooleanType, TimestampType, DateType)
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

# Define input and output namedtuples for job 1
InputGame = namedtuple("InputGame", "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus")
OutputGame = namedtuple("OutputGame", "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus")

def test_job_1(spark_session):
    schema = StructType([
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
        StructField("fg3m", LongType(), True),
        StructField("fg3a", LongType(), True),
        StructField("fg3_pct", DoubleType(), True),
        StructField("ftm", LongType(), True),
        StructField("fta", LongType(), True),
        StructField("ft_pct", DoubleType(), True),
        StructField("oreb", LongType(), True),
        StructField("dreb", LongType(), True),
        StructField("reb", LongType(), True),
        StructField("ast", LongType(), True),
        StructField("stl", LongType(), True),
        StructField("blk", LongType(), True),
        StructField("to", LongType(), True),
        StructField("pf", LongType(), True),
        StructField("pts", LongType(), True),
        StructField("plus_minus", DoubleType(), True)
    ])

    input_data = [(10300001, 1610612762, "UTA", "Utah", 1952, "Raja Bell", None, None, None, "5", 1.0, 5.0, 0.2, 0, 0, 0.0, 0, 0, 0.0, 1, 1, 2, 1, 1, 0, 0, 0, 2, None),
                  (10300001, 1610612762, "UTA", "Utah", 1952, "Raja Bell", None, None, None, "5", 1.0, 5.0, 0.2, 0, 0, 0.0, 0, 0, 0.0, 1, 1, 2, 1, 1, 0, 0, 0, 2, None),
                  (10300001, 1610612762, "UTA", "Utah", 1952, "Raja Bell", None, None, None, "5", 1.0, 5.0, 0.2, 0, 0, 0.0, 0, 0, 0.0, 1, 1, 2, 1, 1, 0, 0, 0, 2, None)]

    expected_output = [(10300001, 1610612762, "UTA", "Utah", 1952, "Raja Bell", None, None, None, "5", 1.0, 5.0, 0.2, 0, 0, 0.0, 0, 0, 0.0, 1, 1, 2, 1, 1, 0, 0, 0, 2, None)]
    input_dataframe = spark_session.createDataFrame(input_data, schema)
    expected_output_dataframe = spark_session.createDataFrame(expected_output, schema)
    actual_df = job_1(spark_session, input_dataframe, "actors")
    
    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)
    
# Define input and output namedtuples for job 2
Web_Events = namedtuple("Web_Events", "user_id device_id referrer host url event_time")
Devices = namedtuple("Devices", "device_id browser_type os_type device_type")
User_Device_Cumulated = namedtuple("User_Device_Cumulated", "user_id browser_type dates_active date")

def test_job_2(spark_session):
    Input_Web_Events = [Web_Events(1078042172, 1088283544, "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer-3-critical-soft-skills-for-success", "www.eczachly.com", "/", "2023-01-01 00:03:24.519"),
                        Web_Events(810784131, 532630305, None, "www.eczachly.com", "/contact", "2023-01-01 22:38:47.614")]

    Input_Devices = [Devices(1088283544, "Other", "Other", "Other"),
                     Devices(532630305, "PetalBot", "Android", "Generic Smartphone")]

    Input_User_Device_Cumulated_yest = [User_Device_Cumulated(810784131, "PetalBot", [date(2022, 12, 31)], date(2023, 1, 1)),
                                        User_Device_Cumulated(1078042172, "Other", [], date(2023, 1, 1))]

    expected_output = [User_Device_Cumulated(810784131, "PetalBot", [date(2023, 1, 1), date(2022, 12, 31)], date(2023, 1, 1)),
                       User_Device_Cumulated(1078042172, "Other", [date(2023, 1, 1)], date(2023, 1, 1))]
    
    web_events_df = spark_session.createDataFrame(Input_Web_Events)
    web_events_df.createOrReplaceTempView("web_events")
    devices_df = spark_session.createDataFrame(Input_Devices)
    devices_df.createOrReplaceTempView("devices")
    user_devices_cumulated_df = spark_session.createDataFrame(Input_User_Device_Cumulated_yest)
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")
    expected_output_df = spark_session.createDataFrame(expected_output)
    actual_df = job_2(spark_session, "user_devices_cumulated")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
