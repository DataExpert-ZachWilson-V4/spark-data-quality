from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DoubleType, LongType, DateType, TimestampType
from collections import namedtuple
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


# This tests if films made by the same actor in the same year are grouped together in the films array
# and if films made by an actor in years different from the current one are excluded from the insertion
def test_job1(spark_session):
    # Step 1: Initialize variables and schemas
    current_year = 1913
    input_table = "actor_films"
    output_table = "actors"
    output_table_schema = StructType(
        [
            StructField("actor", StringType(), True),
            StructField("actor_id", StringType(), True),
            StructField(
                "films",
                ArrayType(
                    StructType(
                        [
                            StructField("film", StringType(), True),
                            StructField("votes", LongType(), True),
                            StructField("rating", DoubleType(), True),
                            StructField("film_id", StringType(), True),
                        ]
                    )
                ),
            ),
            StructField("quality_class", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("current_year", LongType(), True),
        ]
    )

    # Step 2: Create named tuples for both input and output tables
    actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
    actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")
    film = namedtuple("films", "film votes rating film_id")

    # Step 3: Create fake input and dataframe for actor_films table
    input_data = [
        actor_films("Fred Astaire", "nm0000001", "Ghost Story", 1989, 7731, 6.3, "tt0082449"),
        actor_films("Fred Astaire", "nm0000001", "Shall We Dance", 1946, 6603, 7.5, "tt0029546"),
        actor_films("Paul Newman", "nm0000056", "Where the Money Is", current_year + 1, 5896, 6.2, "tt0149367"),
        actor_films("John Cleese", "nm0000092", "Isn't She Great", current_year + 1, 4562, 5.5, "tt0141399"),
        actor_films("John Cleese", "nm0000092", "The Magic Pudding", current_year + 1, 567, 5.9, "tt0141399"),
    ]
    input_data_df = spark_session.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView(input_table)

    # Step 4: Generate expected output based on our input
    # Since this job starts with and empty table, then it filters the year by the current year + 1,
    # it must return two rows, one for each actor that made films in the current year + 1 date
    expected_output = [
        actor(
            actor="Paul Newman",
            actor_id="nm0000056",
            films=[film("Where the Money Is", 5896, 6.2, "tt0149367")],
            quality_class="average",
            is_active=True,
            current_year=current_year + 1,
        ),
        actor(
            actor="John Cleese",
            actor_id="nm0000092",
            films=[film("Isn't She Great", 4562, 5.5, "tt0141399"), film("The Magic Pudding", 567, 5.9, "tt0141399")],
            quality_class="bad",
            is_active=True,
            current_year=current_year + 1,
        ),
    ]
    expected_output_df = spark_session.createDataFrame(data=expected_output, schema=output_table_schema)

    # Step 5: Creating the first iteration of the table since it did not exist in the environment
    initial_output_df = spark_session.createDataFrame(data=[], schema=output_table_schema)
    initial_output_df.createOrReplaceTempView(output_table)

    # Step 6: Running our actual job
    actual_output_df = job_1(spark_session, input_table, output_table, current_year)

    # Step 7: Asserting dataframes equality
    assert_df_equality(actual_output_df, expected_output_df, ignore_row_order=True)


def test_job2(spark_session):
    # Prepare old output data
    output_schema = StructType(
        [
            StructField("host", StringType(), True),
            StructField("host_activity_datelist", ArrayType(DateType()), True),
            StructField("date", DateType(), True),
        ]
    )
    initial_data = [
        {"host": "www.eczachly.com", "host_activity_datelist": [date(2023, 1, 1)], "date": date(2023, 1, 1)},
        {"host": "www.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 1)], "date": date(2023, 1, 1)},
        {"host": "admin.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 1)], "date": date(2023, 1, 1)},
    ]
    output_dataframe = spark_session.createDataFrame(data=initial_data, schema=output_schema)
    output_dataframe.createOrReplaceTempView("hosts_cumulated")

    # Prepare new input data (Today's data)
    input_schema = StructType(
        [
            StructField("host", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    input_data = [
        {"host": "www.eczachly.com", "event_time": datetime(2023, 1, 2)},
        {"host": "www.zachwilson.tech", "event_time": datetime(2023, 1, 2)},
        {"host": "admin.zachwilson.tech", "event_time": datetime(2023, 1, 2)},
    ]
    input_dataframe = spark_session.createDataFrame(data=input_data, schema=input_schema)
    input_dataframe.createOrReplaceTempView("web_events")

    # Execute the job being tested
    actual_df = job_2(spark_session, "web_events", "hosts_cumulated", "2023-01-02")

    # Define expected output for job_2
    expected_output = [
        {"host": "www.eczachly.com", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 1)],
         "date": date(2023, 1, 2)},
        {"host": "www.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 1)],
         "date": date(2023, 1, 2)},
        {"host": "admin.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 1)],
         "date": date(2023, 1, 2)},
    ]

    expected_df = spark_session.createDataFrame(expected_output, schema=output_schema)

    # Assert that the actual DataFrame matches the expected DataFrame
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)
