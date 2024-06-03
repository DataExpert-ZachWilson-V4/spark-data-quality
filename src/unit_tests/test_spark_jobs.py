

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType, DateType,TimestampType

## Unit Test for Job1
NbaGameDetails = namedtuple("NbaGameDetails", "game_id team_id player_id team_abbreviation team_city nickname player_name")
NbaGameDetailsDeduped = namedtuple("NbaGameDetailsDeduped", "game_id team_id player_id team_abbreviation team_city nickname player_name")

GameDetails_schema = StructType([
        StructField("game_id", LongType(), nullable=False),
        StructField("team_id", LongType(), nullable=False),
        StructField("player_id", LongType(), nullable=False),
        StructField("team_abbreviation", StringType(), nullable=False),
        StructField("team_city", StringType(), nullable=False),
        StructField("nickname", StringType(), nullable=True),
        StructField("player_name", StringType(), nullable=False)
    ])

def test_job_1(spark_session):
    input_table_name: str = "nba_game_details"
    input_data = [
        NbaGameDetails(20701032, 1610612758, 201150, "SAC",	"Sacramento", None, "Spencer Hawes"),
        NbaGameDetails(20701032, 1610612758, 201150, "SAC",	"Sacramento", None, "Spencer Hawes"),
        NbaGameDetails(20701032, 1610612758, 201150, "SAC",	"Sacramento", None, "Spencer Hawes"),
        NbaGameDetails(20400555, 1610612753 , 278, "ORL","Orlando",	None, "Stacey Augmon"),
        NbaGameDetails(20400555, 1610612753 , 278, "ORL","Orlando",	None, "Stacey Augmon"),
        NbaGameDetails(1111111, 222222 , 33333, "ORL","Orlando", None, "Unique Player")
       
    ]
    input_df = spark_session.createDataFrame(input_data, GameDetails_schema)

    output_df = job_1(spark_session, input_table_name, input_df)

    expected_data = [
        NbaGameDetails(20701032, 1610612758, 201150, "SAC",	"Sacramento", None, "Spencer Hawes"),
        NbaGameDetails(20400555, 1610612753 , 278, "ORL","Orlando",	None, "Stacey Augmon"),
        NbaGameDetails(1111111, 222222 , 33333, "ORL","Orlando", None, "Unique Player")
    ]
    expected_df = spark_session.createDataFrame(expected_data, GameDetails_schema)

    assert_df_equality(output_df.sort("player_name"), expected_df.sort("player_name"))

## Unit Test for Job2
WebEvent = namedtuple("WebEvent", "user_id device_id referrer host url event_time")
HostCumulated = namedtuple("HostCumulated", "host host_activity_datelist DATE")

HostCumulated_schema = StructType([
        StructField("host", StringType()),
        StructField("host_activity_datelist", ArrayType(DateType(), containsNull=True)),
        StructField("DATE", DateType())
    ])

WebEvent_schema = StructType([
        StructField("user_id", IntegerType(), nullable=True),
        StructField("device_id", IntegerType(), nullable=False),
        StructField("referrer", StringType(), nullable=True),
        StructField("host", StringType(), nullable=False),
        StructField("url", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False)
    ])

def to_dt(time_str: str, is_date: bool=False) -> datetime:
    event_dtf = "%Y-%m-%d %H:%M:%S UTC"
    if is_date:
        event_dtf = "%Y-%m-%d"
    return datetime.strptime(time_str, event_dtf)


def test_job_2(spark_session):
    spark_session.conf.set('spark.sql.session.timeZone', 'UTC')
    cumulated_table_name: str = "hosts_cumulated"
    daily_table_name: str = "web_events"
    today = "2023-01-02"
    
    cumulated_data = [
        HostCumulated("www.eczachly.com", [datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-01", "%Y-%m-%d")),
        HostCumulated("www.zachwilson.tech", [datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-01", "%Y-%m-%d")),
        HostCumulated("admin.zachwilson.tech", [datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-01", "%Y-%m-%d"))

    ]

    daily_data = [
        WebEvent(749228935, -2012543895, None, "www.zachwilson.tech", "/", datetime.strptime("2023-01-02 07:00:34.627 UTC", "%Y-%m-%d %H:%M:%S.%f UTC")),
        WebEvent(-2077270748, -2012543895, None, "www.zachwilson.tech", "/", datetime.strptime("2023-01-02 07:15:32.122 UTC", "%Y-%m-%d %H:%M:%S.%f UTC")),
        WebEvent(1917610522, -290659081, None, "www.eczachly.com", "/robots.txt", datetime.strptime("2023-01-02 07:05:00.648 UTC", "%Y-%m-%d %H:%M:%S.%f UTC"))
    ]

    cumulated_df = spark_session.createDataFrame(cumulated_data,HostCumulated_schema)
    daily_df = spark_session.createDataFrame(daily_data,WebEvent_schema)
    
    output_df = job_2(spark_session, cumulated_df,cumulated_table_name,
                      daily_df,daily_table_name,today)

    expected_data = [
        HostCumulated("www.eczachly.com", [datetime.strptime("2023-01-02", "%Y-%m-%d"),datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-02", "%Y-%m-%d")),
        HostCumulated("www.zachwilson.tech", [datetime.strptime("2023-01-02", "%Y-%m-%d"),datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-02", "%Y-%m-%d")),
        HostCumulated("admin.zachwilson.tech", [None,datetime.strptime("2023-01-01", "%Y-%m-%d")], datetime.strptime("2023-01-02", "%Y-%m-%d"))
    ]
    expected_df = spark_session.createDataFrame(expected_data, HostCumulated_schema)

    assert_df_equality(output_df.sort("host"), expected_df.sort("host"))
