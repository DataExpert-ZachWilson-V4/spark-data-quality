from collections import namedtuple
from datetime import date

from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from tests.conftest import spark_session


# Ref: review lab day 3
def test_job_1_game_dedup(spark_session):
    """
    Test for job1 dedup nba_game_details
    Note: see fact-data-modelling query 1
    """

    game_detail = namedtuple(
        "game_detail",
        "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct"
    )

    # contains duplicates
    input_data = [
        game_detail(
            game_id=11080016,
            team_id=1610787738,
            team_abbreviation="NYK",
            team_city="New York",
            player_id=201186,
            player_name="John Doe",
            nickname="JD",
            start_position="Forward",
            comment="Great game!",
            min="35:20",
            fgm=1.0,
            fga=20.0,
            fg_pct=50.0
        ),
        game_detail(
            11080016, 1610787738, "NYK", "New York", 201186, "John Doe",
            "JD", "Forward", "Great game!", "35:20", 1.0, 20.0, 50.0
        )
    ]
    expected_output = [
        game_detail(
            11080016, 1610787738, "NYK", "New York", 201186, "John Doe",
            "JD", "Forward", "Great game!", "35:20", 1.0, 20.0, 50.0
        )
    ]

    # pyspark schema
    game_detail_schema = StructType(
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
        ]
    )

    input_df = spark_session.createDataFrame(input_data, schema=game_detail_schema)
    input_df.createOrReplaceTempView("nba_game_details")
    expected_output_df = spark_session.createDataFrame(expected_output, schema=game_detail_schema)

    actual_df = job_1(spark_session, "nba_game_details")

    assert_df_equality(expected_output_df, actual_df, ignore_nullable=True, ignore_row_order=True)


def test_job_2_user_devices_cumulated(spark_session):
    """
    Test for job2 device cumulated
    Note: see fact-data-modelling query 3

    TODO: Haven't built spark schemas for this test, save it for future exercise
    TODO: Note: that this job processes 3 tables, we want to keep a test focussed on single responsibility.
        However, we could (and should) update our job query to accept kwargs for the other 2,
        and then write 2 more table focussed unit tests
    Note: TA (Joe) please don't dock me for not doing the afore mentioned -- I'm maxed out btwn assembly and c programing
    at my uni data structs algos and architecture classes and im working full time as well.
    Please have mercy!!!
    I really intend on working more w/ this module and adding more tests
    """
        
    web_event = namedtuple("web_event", "user_id device_id referrer host url event_time")
    device = namedtuple("device", "device_id browser_type os_type device_type")
    device_cumulated = namedtuple("device_cumulated", "user_id browser_type dates_active date")

    input_web_events = [
        web_event(587642973, 9121527308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2022-01-02 11:34:14.204 UTC")
    ]

    input_devices = [
        device(9121527308, "Chrome", "Mac OS X", "Other")
    ]

    input_devices_cumulated = [
        device_cumulated(
            587642973, "Chrome", [date(2022, 1, 1)], date(2022, 1, 2)
        )
    ]

    test_devices_cumulated_df = spark_session.createDataFrame(input_devices_cumulated)
    test_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    test_web_events_df = spark_session.createDataFrame(input_web_events)
    test_web_events_df.createOrReplaceTempView("web_events")

    test_input_devices_df = spark_session.createDataFrame(input_devices)
    test_input_devices_df.createOrReplaceTempView("devices")

    # note: not passing in existing input_devices_cumulated
    expected_output = [
        device_cumulated(587642973, "Chrome", [date(2022, 1, 2)], date(2022, 1, 2))
    ]
    expected_output_df = spark_session.createDataFrame(expected_output)

    actual_df = job_2(spark_session, "web_events")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
