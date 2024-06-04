from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple
from datetime import date

def test_game_dedupe(spark):
    game_details = namedtuple("game_details", "game_id team_id team_abbreviation team_city player_id player_name nickname start_position")

    input_data = [
        game_details(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null),
        game_details(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null)
    ]

    expected_output = [
        game_details(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null)
    ]

    input_df = spark.createDataFrame(input_data)
    input_df.createOrReplaceTempView("game_details")

    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, bootcamp.nba_game_details)
    assert_df_equality = (actual_df, expected_output_df, ignore_nullable=true)

def test_user_devices_cumulative_table(spark):
    web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
    devices = namedtuple("devices", "device_id browser_type os_type device_type")
    devices_cumulated = namedtuple("devices_cumulated", "user_id browser_type dates_active date")

    input_devices_cumulated = [
        devices_cumulated(-557690973, "Chrome", [date(2023,1,1)],date(2023,1,1))
    ]

    input_web_events = [
        web_events(-557690973, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-02 08:34:14.204 UTC")
    ]

    input_devices = [
        devices(-2052907308, "Chrome", "Mac OS X", "Other")
    ]

    test_devices_cumulated_df = spark.createDataFrame(input_devices_cumulated)
    test_devices_cumulated_df.createOrReplaceTempView("devices_cumulated")

    test_web_events_df = spark.createDataFrame(input_web_events)
    test_web_events_df.createOrReplaceTempView("web_events")

    test_input_devices_df = spark.createDataFrame(input_devices)
    test_input_devices_df.createOrReplaceTempView("devices")

    expected_output = [
        devices_cumulated(-557690973, "Chrome", [date(2023,1,1), date(2023,1,2)],date(2023,1,2))
    ]
    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, ameena543246912.user_devices_cumulated)
    assert_df_equality = (actual_df, expected_output_df, ignore_nullable=true)