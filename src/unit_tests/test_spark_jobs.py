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

def test_job_2(spark_session):
    # Create DataFrames for user_devices_cumulated and web_events + devices for testing
    web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
    devices = namedtuple("devices", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

        # Input data for testing
input_events_data = [
            web_events(
                user_id = 495022226,
                device_id = -2012543895, 
                referrer = NULL, 
                host = 'www.zachwilson.tech', 
                url = '/', 
                event_time = datetime(2023, 1, 3, 0, 31, tzinfo=pytz.UTC)
            )
        ]

input_devices_data = [
            devices(
                device_id = -2012543895, 
                browser_type = 'Googlebot', 
                os_type = 'Other', 
                device_type = 'Spider'
            )
        ]

# Existing data for testing
        
existing_user_devices_cumulated_data = [
            user_devices_cumulated(
                user_id = 495022226,
                browser_type = 'Googlebot',
                dates_active = [datetime.strptime("2023-01-02", "%Y-%m-%d").date()],
                date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()
            )
        ]

# Expected data for testing
expected_data = [
            user_devices_cumulated(
                user_id = 495022226,
                browser_type = 'Googlebot',
                dates_active = [datetime.strptime("2023-01-03", "%Y-%m-%d").date(), datetime.strptime("2023-01-02", "%Y-%m-%d").date()],
                date = datetime.strptime("2023-01-03", "%Y-%m-%d").date()
            )
        ]

        
# DataFrame of the input and existing data and views the test will reference
input_events_df = spark_session.createDataFrame(input_events_data)
input_events_df.createOrReplaceTempView("web_events")

input_devices_df = spark_session.createDataFrame(input_devices_data)
input_devices_df.createOrReplaceTempView("devices")

existing_df = spark_session.createDataFrame(existing_user_devices_cumulated_data)
existing_df.createOrReplaceTempView("user_devices_cumulated")

# Run the Update_User_Devices_Cumulated function and compare the output with expected
actual_df = Update_User_Devices_Cumulated(spark_session, "user_devices_cumulated")
expected_df = spark_session.createDataFrame(expected_data)
assert_df_equality(actual_df, expected_df, ignore_nullable=True)        
        
