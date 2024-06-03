import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DoubleType, LongType
from collections import namedtuple
from src.jobs.job_1 import job_1


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
