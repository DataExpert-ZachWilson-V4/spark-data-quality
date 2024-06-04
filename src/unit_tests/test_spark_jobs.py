from chispa.dataframe_comparer import assert_df_equality
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from collections import namedtuple
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, ArrayType, StringType, LongType
)

import pytest

# Define namedtuples
userdevices = namedtuple("userdevices", "user_id browser_type dates_active date")
userdevicesbitwise = namedtuple("userdevicesbitwise", "user_id browser_type history_int history_in_binary")
InputCumulative = namedtuple("InputCumulative", "host host_activity_datelist date")
InputEvents = namedtuple("InputEvents", "host event_time")
OutputCumulative = namedtuple("OutputCumulative", "host host_activity_datelist date")

# Define schemas
userdevices_schema = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("browser_type", StringType(), nullable=True),
    StructField("dates_active", ArrayType(StringType(), containsNull=True), nullable=True),
    StructField("date", StringType(), nullable=False)
])

userdevicesbitwise_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("browser_type", StringType(), True),
    StructField("history_int", LongType(), True),
    StructField("history_in_binary", StringType(), True)
])

cumulative_schema = StructType([
    StructField("host", StringType(), nullable=True),
    StructField("host_activity_datelist", ArrayType(StringType(), containsNull=True), nullable=True),
    StructField("date", StringType(), nullable=False)
])



def test_job_1(spark_session):
    input_table_name = "user_devices_cumulated"
    source_data = [
        userdevices("-1358803869", "YandexBot", ["2023-01-01"], "2023-01-01"),
        userdevices("-2077270748", "Googlebot", ["2023-01-01"], "2023-01-01"),
        userdevices("1998252220", "Googlebot", ["2023-01-01"], "2023-01-01"),
        userdevices("1952553454", "SemrushBot", ["2023-01-01"], "2023-01-01")
    ]
    source_df = spark_session.createDataFrame(source_data, userdevices_schema)

    actual_df = job_1(spark_session, source_df, input_table_name)
    expected_data = [
        userdevicesbitwise("-1358803869", "YandexBot", 1073741824, "1000000000000000000000000000000"),
        userdevicesbitwise("-2077270748", "Googlebot", 1073741824, "1000000000000000000000000000000"),
        userdevicesbitwise("1998252220", "Googlebot", 1073741824, "1000000000000000000000000000000"),
        userdevicesbitwise("1952553454", "SemrushBot", 1073741824, "1000000000000000000000000000000")
    ]
    expected_df = spark_session.createDataFrame(expected_data,userdevicesbitwise_schema)

    assert_df_equality(actual_df.sort("user_id", "browser_type"), expected_df.sort("user_id", "browser_type"))


def test_job_2(spark_session):
    cumulated_table_name = "hosts_cumulated"
    event_table_name = "web_events"

    input_cumulative_data = [
        InputCumulative("www.zachwilson.tech", ["2023-01-01"], "2023-01-01"),
        InputCumulative("www.eczachly.com", ["2023-01-01"], "2023-01-01"),
        InputCumulative("admin.zachwilson.tech", ["2023-01-01"], "2023-01-01")
    ]
    cumulated_df = spark_session.createDataFrame(input_cumulative_data, cumulative_schema)

    input_event_data = [
        InputEvents("www.zachwilson.tech", "2023-01-02 00:00:00"),
        InputEvents("www.eczachly.com", "2023-01-02 09:00:00"),
        InputEvents("admin.zachwilson.tech", "2023-01-02 10:00:00")
    ]
    event_df = spark_session.createDataFrame(input_event_data)

    actual_df = job_2(spark_session, cumulated_df, cumulated_table_name, event_df, event_table_name)

    expected_data = [
        OutputCumulative("www.zachwilson.tech", ["2023-01-02", "2023-01-01" ], "2023-01-02"),
        OutputCumulative("www.eczachly.com", ["2023-01-02", "2023-01-01" ], "2023-01-02"),
        OutputCumulative("admin.zachwilson.tech", ["2023-01-02", "2023-01-01" ], "2023-01-02")
    ]
    expected_df = spark_session.createDataFrame(expected_data, cumulative_schema)

    assert_df_equality(actual_df.sort("host", "date"), expected_df.sort("host", "date"))