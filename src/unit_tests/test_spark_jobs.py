import pytest
from datetime import date
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType, StructField
from collections import namedtuple

# Local import assuming test running from a root directory with module installed as `pip install -e .`
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

# Fixtures
def spark_session_factory(app_name: str) -> SparkSession:
  return (
      SparkSession.builder
      .master("local")
      .appName("chispa")
      .getOrCreate()
  )

@pytest.fixture(scope='session')
def spark_session():
    return spark_session_factory("spark_unit_tests")

def test_1(spark_session):

    actors_cumulated = namedtuple("actor", "actor_id", "YEAR", "avg_rating", ["film", "votes", "rating", "film_id"])
    actor_films = namedtuple("actor", "actor_id", "film", "year", "votes", "rating", "film_id")

    # Define web_events input data
    actor_films_input_data = [
        actor_films("Humphrey Bogart", "nm0000007", "Up the River", 1930, 1095, 6, "tt0021508"),
        actor_films("James Cagney", "nm0000010", "The Doorway to Hell", 1930, 1003, 6.5, "tt0020836"),
    ]

    actor_films_input_data_df = spark_session.createDataFrame(actor_films_input_data)
    actor_films_input_data_df.createOrReplaceTempView("actor_films")

    actors_cumulated_df = spark_session.createDataFrame(
        data=[],
        schema=StructType([
            StructField("actor", StringType(), True),
            StructField("actor_id", StringType(), True),
            StructField("YEAR", IntegerType(), True),
            StructField("avg_rating", StringType(), True),   
            StructField("film", ArrayType(StringType()), True),
        ])
    )

    actors_cumulated_df.createOrReplaceTempView("actors_cumulated")


    expected_output_data = [
        actors_cumulated("Humphrey Bogart", "nm0000007", ["Up the River", 1095, 6, "tt0021508"], "bad", True, 1930),
        actors_cumulated("James Cagney", "nm0000010", ["The Doorway to Hell", 1003, 6.5, "tt0020836"], "average", True, 1930),
    ]

    expected_output_data_df = spark_session.createDataFrame(expected_output_data)
    actual_output_data_df = job_1(spark_session, "actors_cumulated")
    assert_df_equality(actual_output_data_df.sort("actor"), expected_output_data_df)

def test_2(spark_session):

    web_events = namedtuple("web_events", ["user_id", "device_id", "event_time"])
    devices = namedtuple("devices", ["device_id", "browser_type"])


    web_events_input_data = [

        web_events(1379581694, 532630305, "2021-03-24 16:25:58.531"),
        web_events(1379581694, 532630305, "2021-03-25 16:25:58.531"),
        web_events(1379581694, 532630306, "2021-03-26 00:00:00"),
        web_events(1981621255, 1059431278, "2021-03-27 16:25:58.531"),
        web_events(1981621255, 1059431278, "2021-03-28 16:25:58.531"),
        web_events(1981621255, 1059431279, "2021-03-29 16:25:58.531"),
    ]

    web_events_input_data_df = spark_session.createDataFrame(web_events_input_data)
    web_events_input_data_df.createOrReplaceTempView("web_events")

    # Define devices input data
    devices_input_data = [
        devices(532630305, "device 532630305"),
        devices(532630306, "device 532630306"),
        devices(1059431278, "device 1059431278"),
        devices(1059431279, "device 1059431279"),
    ]

    devices_input_data_df = spark_session.createDataFrame(devices_input_data)
    devices_input_data_df.createOrReplaceTempView("devices")

    user_devices_cumulated = namedtuple("user_devices_cumulated", ["user_id", "browser_type", "dates_active", "date"])

    initial_user_devices_cumulated_df = spark_session.createDataFrame(
        data=[],
        schema=StructType([
            StructField("user_id", IntegerType(), True),
            StructField("browser_type", StringType(), True),
            StructField("dates_active", ArrayType(StringType()), True),
            StructField("date", StringType(), True),
        ])
    )

    initial_user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    expected_output_data = [
        user_devices_cumulated(532630305, "device 532630305", ["2021-03-24", "2021-03-25"], "2021-03-29"),
        user_devices_cumulated(532630306, "device 532630306", ["2021-03-26"], "2021-03-29"),
        user_devices_cumulated(1059431278, "device 1059431278", ["2021-03-27", "2021-03-28"], "2021-03-29"),
        user_devices_cumulated(1059431279, "device 1059431278", ["2021-03-29"], "2021-03-29"),
    ]

    expected_output_data_df = spark_session.createDataFrame(expected_output_data)
    actual_output_data_df = job_2(spark_session, "user_devices_cumulated")
    assert_df_equality(actual_output_data_df.sort("user_id"), expected_output_data_df)
