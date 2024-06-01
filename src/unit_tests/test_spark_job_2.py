from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from collections import namedtuple
from chispa.dataframe_comparer import  assert_df_equality
from ..jobs import job_2

def test_inc_host_data(spark_session):
    host_cols = ["host", "host_activity_datelist", "date"]
    Hosts = namedtuple("Hosts", host_cols)
    web_event_cols = ["host", "event_time"]
    WebEvents = namedtuple("WebEvents", web_event_cols)

    host_schema = StructType([
        StructField("host", StringType()),
        StructField("host_activity_datelist", ArrayType(DateType())),
        StructField("date", DateType())
    ])

    web_event_schema = StructType([
        StructField("host", StringType()),
        StructField("event_time", StringType())
    ])

    actual_host_data = []
    actual_web_event_data = [
        WebEvents(
            host = "www.zachwilson.tech",
            event_time = "2023-01-01 21:29:03.519"
        ),
        WebEvents(
            host = "www.zachwilson.tech",
            event_time = "2023-01-01 21:29:05.112"
        ),
        WebEvents(
            host = "www.eczachly.com",
            event_time = "2023-01-01 00:01:39.907"
        ),
        WebEvents(
            host = "www.eczachly.com",
            event_time = "2023-01-01 00:03:24.519"
        ),
        WebEvents(
            host = "admin.zachwilson.tech",
            event_time = "2023-01-01 00:08:13.852"
        ),
        WebEvents(
            host = "admin.zachwilson.tech",
            event_time = "2023-01-01 02:12:38.139"
        ),

    ]

    expected_data = [
        Hosts(
            host = "www.eczachly.com",
            host_activity_datelist = [datetime.strptime("2023-01-01", "%Y-%m-%d")],
            date = datetime.strptime("2023-01-01", "%Y-%m-%d")
        ),
        Hosts(
            host = "admin.zachwilson.tech",
            host_activity_datelist = [datetime.strptime("2023-01-01", "%Y-%m-%d")],
            date = datetime.strptime("2023-01-01", "%Y-%m-%d")
        ),
        Hosts(
            host = "www.zachwilson.tech",
            host_activity_datelist = [datetime.strptime("2023-01-01", "%Y-%m-%d")],
            date = datetime.strptime("2023-01-01", "%Y-%m-%d")
        )
    ]

    spark = spark_session()
    host_df = spark.createDataFrame(actual_host_data, schema=host_schema)
    web_events_df = spark.createDataFrame(actual_web_event_data, schema=web_event_schema)
    expected_df = spark.createDataFrame(expected_data, schema=host_schema)

    host_df.createOrReplaceTempView("host_cumulated")
    web_events_df.createOrReplaceTempView("web_events")
    # host_df.show()
    # web_events_df.show(truncate=False)
    # expected_df.show()

    actual_df = job_2.job_2(
        spark, 
        "host_cumulated",
        "web_events",
        "2023-01-01",
        "host_cumulated"
    )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)