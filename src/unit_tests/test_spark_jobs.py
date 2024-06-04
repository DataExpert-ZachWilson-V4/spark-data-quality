import sys
import os
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import date
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, IntegerType, DateType, TimestampType

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")

def test1(spark_session):
    
    input_actors = [
        actor_films("Rowan Atkinson","nm0000100","Rat Race",2001,109450,6.4,"tt0250687")
    ]

    expected_actors = [
        actors(
            "Rowan Atkinson",
            "nm0000100",
            [["Rat Race", 109450, 6.4, "tt0250687", 2001]],
            "average",
            True,
            2001
        )
    ]

    last_year_actors = [
        actors(
        "Parker Posey",
        "nm0000391",
        [["Josie and the Pussycats",21859,5.4,"tt0236348",2001],["The Anniversary Party",8024,6.3,"tt0254099",2001]],
        "bad",
        True,
        2001
        )
    ]
    
    input_df = spark_session.createDataFrame(input_actors)
    input_df.createOrReplaceTempView("actor_films")
    last_year_actor_df = spark_session.createDataFrame(last_year_actors)
    last_year_actor_df.createOrReplaceTempView("actors")
    expected_actors_df = spark_session.createDataFrame(expected_actors)
    
    actual_df = job_1(spark_session, "actors")
    assert_df_equality(actual_df, expected_actors_df, ignore_nullable=True)


hosts_cumulated = namedtuple("hosts_cumulated", "host host_activity_datelist date")
web_events = namedtuple("WebEvents", "user_id device_id referrer host url event_time")

def test2(spark_session):

    host_cumulated_input = [
        hosts_cumulated("www.zachwilson.tech", [date(2023,1,1)], date(2023,1,1))
    ]

    hc_df = spark_session.createDataFrame(host_cumulated_input)
    hc_df.createOrReplaceTempView("hosts_cumulated")

    input_data_web_events = [    
        web_events(-637142783, 1993067696, '', "www.zachwilson.tech", "/", "2021-06-03 03:41:12.265 UTC")
    ]
     
    we_df = spark_session.createDataFrame(input_data_web_events)
    we_df.createOrReplaceTempView("web_events")

   
    expected_output = [
        hosts_cumulated("www.zachwilson.tech", [date(2023, 1, 1)], date(2023, 1, 1))
    ]
    expected_df = spark_session.createDataFrame(host_cumulated_input)

    actual_df2 = job_2(spark_session, "hosts_cumulated")
    assert_df_equality(actual_df2, expected_df, ignore_nullable=False)
