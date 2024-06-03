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

@pytest.fixture
def current_date_job_1() -> str:
    return date(2021,1,1)

@pytest.fixture
def current_date_job_2() -> str:
    return date(2021,1,7)

def test_job_1(spark_session, current_date_job_1):
    """This function exemplifies a test for the job_1 spark function using Pytest and Chispa.
    The tests included here are not meant to be comprehensive, but rather to provide a starting point.
    """
    # %% Input data
    # Define input data headers
    web_events = namedtuple("web_events", ["user_id", "device_id", "event_time"])
    devices = namedtuple("devices", ["device_id", "browser_type"])

    # Define web_events input data
    web_events_input_data = [
        # Testing user 1 with device 1 from 2021-01-01 to 2021-01-02
        # And switching to device 2 on 2021-01-03
        web_events(1, 1, "2021-01-01 00:00:00"),
        web_events(1, 1, "2021-01-02 00:00:00"),
        web_events(1, 2, "2021-01-03 00:00:00"),
        # Testing user 2 with device 2 from 2021-01-01 to 2021-01-02
        # And switching to device 1 on 2021-01-03
        web_events(2, 2, "2021-01-01 00:00:00"),
        web_events(2, 2, "2021-01-02 00:00:00"),
        web_events(2, 1, "2021-01-03 00:00:00"),
    ]

    web_events_input_data_df = spark_session.createDataFrame(web_events_input_data)
    web_events_input_data_df.createOrReplaceTempView("web_events")

    # Define devices input data
    devices_input_data = [
        devices(1, "Chrome"),
        devices(2, "Safari"),
    ]

    devices_input_data_df = spark_session.createDataFrame(devices_input_data)
    devices_input_data_df.createOrReplaceTempView("devices")

    # %% Expected output data
    # Define expected output data headers
    user_devices_cumulated = namedtuple("user_devices_cumulated", ["user_id", "browser_type", "dates_active", "date"])

    # Initialize the output table
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
        user_devices_cumulated(1, "Chrome", ["2021-01-01"], current_date_job_1),
        user_devices_cumulated(2, "Safari", ["2021-01-01"], current_date_job_1),
    ]

    expected_output_data_df = spark_session.createDataFrame(expected_output_data)
    actual_output_data_df = job_1(spark_session, "user_devices_cumulated", current_date_job_1)
    assert_df_equality(actual_output_data_df.sort("user_id"), expected_output_data_df)

def test_job_2(spark_session, current_date_job_1):
    """This function exemplifies a test for the job_2 spark function using Pytest and Chispa.
    The tests included here are not meant to be comprehensive, but rather to provide a starting point.
    """
    # %% Input data
    # Define input data headers
    hosts_cumulated = namedtuple("hosts_cumulated", ["host", "host_activity_datelist", "date"])
    web_events = namedtuple("web_events", ["user_id", "device_id", "host", "event_time"])

    # Define web_events input data
    web_events_input_data = [
        web_events(1, 1, "www.eczachly.com", "2021-01-01 00:00:00"),
        web_events(2, 1, "www.zachwilson.tech", "2021-01-01 00:00:00"),
    ]

    web_events_input_data_df = spark_session.createDataFrame(web_events_input_data)
    web_events_input_data_df.createOrReplaceTempView("web_events")

    # %% Expected output data
    # Initialize the output table
    hosts_cumulated_df = spark_session.createDataFrame(
        data=[],
        schema=StructType([
            StructField("host", StringType(), True),
            StructField("host_activity_datelist", ArrayType(StringType()), True),
            StructField("date", StringType(), True),
        ])
    )

    hosts_cumulated_df.createOrReplaceTempView("hosts_cumulated")

    # Expected output data
    expected_output_data = [
        hosts_cumulated("www.eczachly.com", ["2021-01-01"], current_date_job_1),
        hosts_cumulated("www.zachwilson.tech", ["2021-01-01"], current_date_job_1),
    ]

    expected_output_data_df = spark_session.createDataFrame(expected_output_data)
    actual_output_data_df = job_2(spark_session, "hosts_cumulated", current_date_job_1)
    assert_df_equality(actual_output_data_df.sort("host"), expected_output_data_df)