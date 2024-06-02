import sys
sys.path.append('/Users/renee/Desktop/education/dataengineering/spark-data-quality-WhiskersReneeWe/src/')

from pyspark.sql import Row
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType,BooleanType, DateType
from collections import namedtuple
from src.jobs import job_1 as j1
from src.jobs import job_2 as j2

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark_session():
    return (
      SparkSession.builder
      .master("local")
      .appName("chispa")
      .getOrCreate()
  )

# spart_session is a pytest fixture
def test_job_1(spark_session):

    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    test_actor_history_scd = namedtuple("test_actor_history_scd", "actor actor_id quality_class is_active start_date end_date current_year")

    input_data = [
        actors(actor="Jack Gerber", actor_id="dhdne111", films=[["Hot Water",1258,7.1,"tt0015002"]], quality_class=1, is_active=0, current_year='2022'),
        actors(actor="Penny Franklin", actor_id="pendf211", films = [["The Story of Alexander Graham Bell",990,6.9,"tt0031981"]], quality_class=2, is_active=1, current_year='2022')
    ]
    
    input_df = spark_session.createDataFrame(input_data)

    # create a temp table from input_df
    input_df.createOrReplaceTempView("test_actors")
    test_output_table_name = "test_actors"
    
    actual_df = j1.job_1(spark_session, test_output_table_name)

    print(f"Resulting Schema {actual_df.schema}")

    expected_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("quality_class", LongType(), True),
        StructField("is_active", LongType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("current_year", StringType(), True)
    ])

    assert actual_df.schema == expected_schema


def test_job_2 (spark_session):

    yesterday = namedtuple("yesterday", "host metric_name metric_array month_start")
    fake_yesterday_input=[
        yesterday(host="zachwilson.techcreator.io", metric_name="visited_home_page", metric_array=[6], month_start=date(2023, 8, 1)),
        yesterday(host="www.eczachly.com", metric_name="visited_home_page", metric_array=[120], month_start=date(2023, 8, 1)),
        yesterday(host="www.zachwilson.tech", metric_name="visited_home_page", metric_array=[124,124], month_start=date(2023, 8, 1))
    ]

    today = namedtuple("today", "host metric_name metric_value date")
    fake_today_input=[
        today(host="zachwilson.techcreator.io", metric_name="visited_home_page", metric_value=13, date=date(2023, 8, 21)),
        today(host="www.eczachly.com", metric_name="visited_home_page", metric_value=297, date=date(2023, 8, 21)),
        today(host="www.zachwilson.tech", metric_name="visited_home_page", metric_value=204, date=date(2023, 8, 21))

    ]

    yesterday_df = spark_session.createDataFrame(fake_yesterday_input)

    # create a temp table from input_df
    yesterday_df.createOrReplaceTempView("test_yesterday")
    test_output_table_name_1 = "test_yesterday"

    today_df = spark_session.createDataFrame(fake_today_input)

    # create a temp table from input_df
    today_df.createOrReplaceTempView("test_today")
    test_output_table_name_2 = "test_today"

    actual_df = j2.job_2(spark_session, test_output_table_name_1, test_output_table_name_2)

    print(f"Resulting Schema {actual_df.schema}")

    expected_schema = StructType([
        StructField("host", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_array", ArrayType(LongType()), True),
        StructField("month_start", DateType(), True)
    ])

    assert actual_df.schema == expected_schema


    
