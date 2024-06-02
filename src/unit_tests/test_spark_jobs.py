from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, ArrayType, StringType, IntegerType, BooleanType, DateType, Row, DoubleType
)
from datetime import date
import pytest
from chispa.dataframe_comparer import assert_df_equality
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a Spark session for tests with cleanup post testing."""
    spark = SparkSession.builder.master("local").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()

def test_actors_aggregation(spark):
    """Test to ensure correct aggregation and data handling in the actors dataset."""
    # Define a schema based on the dataset structure, including nested types for film details
    film_info_type = StructType([
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("film_id", StringType(), True),
        StructField("year", IntegerType(), True)
    ])

    schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("films", ArrayType(film_info_type), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), True)
    ])

    # Creating sample data for testing
    input_data = [
        Row(
            actor="Wallace Langham", actor_id="nm0005120",
            films=[Row(film="Operation Varsity Blues: The College Admissions Scandal", votes=4330, rating=7, film_id="tt1117734", year=2021)],
            quality_class="average", is_active=True, current_year=2021
        ),
        Row(
            actor="Jesse Eisenberg", actor_id="nm0251986",
            films=[
                Row(film="Zack Snyder's Justice League", votes=242474, rating=8.2, film_id="tt12361974", year=2021),
                Row(film="Wild Indian", votes=121, rating=6.8, film_id="tt1433048", year=2021)
            ],
            quality_class="good", is_active=True, current_year=2021
        )
    ]

    input_dataframe = spark.createDataFrame(input_data, schema=schema)
    input_table_name = "actors"
    input_dataframe.createOrReplaceTempView(input_table_name)

    # Execute the job being tested
    actual_df = job_1(spark, input_table_name)

    # Define expected output based on known transformation rules
    expected_output = [
        Row(
            actor="Wallace Langham", actor_id="nm0005120",
            films=[Row(film="Operation Varsity Blues: The College Admissions Scandal", votes=4330, rating=7, film_id="tt1117734", year=2021)],
            quality_class="average", is_active=True, current_year=2021
        ),
        Row(
            actor="Jesse Eisenberg", actor_id="nm0251986",
            films=[
                Row(film="Zack Snyder's Justice League", votes=242474, rating=8.2, film_id="tt12361974", year=2021),
                Row(film="Wild Indian", votes=121, rating=6.8, film_id="tt1433048", year=2021)
            ],
            quality_class="star", is_active=True, current_year=2021
        )
    ]

    expected_df = spark.createDataFrame(expected_output, schema=schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_hosts_cumulation(spark):
    """Test to verify the correct cumulation of host activity over dates."""
    # Define schema to reflect the structure of the hosts_cumulated table
    schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True),
    ])

        # Prepare test data
    input_data = [
        {"host": "www.eczachly.com", "host_activity_datelist": [date(2023, 1, 2)], "date": date(2023, 1, 2)},
        {"host": "www.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2)], "date": date(2023, 1, 2)},
        {"host": "admin.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2)], "date": date(2023, 1, 2)}
    ]

    input_dataframe = spark.createDataFrame(input_data, schema=schema)
    input_table_name = "hosts_cumulated"
    input_dataframe.createOrReplaceTempView(input_table_name)

    # Execute the job being tested
    actual_df = job_2(spark, input_table_name)

    # Define expected output based on hypothetical transformations by job_2
    expected_output = [
        {"host": "www.eczachly.com", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 3)], "date": date(2023, 1, 3)},
        {"host": "www.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 3)], "date": date(2023, 1, 3)},
        {"host": "admin.zachwilson.tech", "host_activity_datelist": [date(2023, 1, 2), date(2023, 1, 3)], "date": date(2023, 1, 3)}
    ]

    expected_df = spark.createDataFrame(expected_output, schema=schema)

    # Assert that the actual DataFrame matches the expected DataFrame
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
