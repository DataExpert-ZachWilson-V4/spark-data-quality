from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType, DataType, IntegerType
from pyspark.sql import Row

from src.jobs.job_1 import job_1, current_year
from src.jobs.job_2 import job_2

def test_actors_table(spark):
    # unit tests
    #  - for input actor_films if year is not current_year + 1
    #  - for input actors_df if last year films is not null and this year film is not null

    input_data_actor_films = [
        actor_films(
            "Adam Sandler",
            "nm0001191",
            "Little Nicky",
            current_year + 1,
            99677,
            5.3,
            "tt0185431",
        ),
        actor_films(
            "Adam Goldberg",
            "nm0004965",
            "Sunset Strip",
            current_year + 1,
            1473,
            5.7,
            "tt0178050",
        ),
        actor_films(
            "Adam Sandler",
            "nm000119",
            "Big Daddy",
            current_year,
            201507,
            6.4,
            "tt0142342",
        ),
    ]
    temp_actor_films_df = spark.createDataFrame(input_data_actor_films)
    temp_actor_films_df.createOrReplaceTempView("actor_films")

    input_data_actors = [
        actor(
            "Adam Sandler",
            "nm0000119",
            [
                Row(
                    year=current_year,
                    film="Big Daddy",
                    votes=201507,
                    rating=6.4,
                    film_id="tt0142342",
                )
            ],
            "average",
            True,
            current_year,
        )
    ]
    temp_actors_df = spark.createDataFrame(input_data_actors)
    temp_actors_df.createOrReplaceTempView("actors")

    # expected output based on our input
    expected_output = [
        actor(
            "Adam Sandler",
            "nm0000119",
            [
                Row(
                    year=current_year,
                    film="Big Daddy",
                    votes=201507,
                    rating=6.4,
                    film_id="tt0142342",
                ),
                Row(
                    year=current_year + 1,
                    film="Little Nicky",
                    votes=99677,
                    rating=5.3,
                    film_id="tt0185431",
                ),
            ],
            "bad",
            True,
            current_year + 1,
        ),
        actor(
            "Adam Goldberg",
            "nm0004965",
            [
                Row(
                    year=current_year + 1,
                    film="Sunset Strip",
                    votes=1473,
                    rating=5.7,
                    film_id="tt0178050",
                )
            ],
            "bad",
            True,
            current_year + 1,
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actors", current_year)

    # This argument verifies that dataframes are the same
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job_2(spark):
    # This unit test will check data types for the schema
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
   
    input_data = [
        actors(actor="Adam Sandler", actor_id="nm0001191", films=[["Little Nicky",99677,5.3,"tt0185431"]], quality_class='bad', is_active=1, current_year='2000'),
        actors(actor="Adam Goldberg", actor_id="nm0004965", films = [["Sunset Strip",1473,5.7,"tt0178050"]], quality_class='bad', is_active=1, current_year='2000')
    ]

    input_df = spark.createDataFrame(input_data)

    # Temp table for the df
    input_df.createOrReplaceTempView("test_actors")
    test_output_table_name = "test_actors"

    actual_df = job_2(spark, test_output_table_name)

    print(f"Resulting Schema {actual_df.schema}")

    expected_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("start_date", IntegerType(), True),
        StructField("end_date", IntegerType(), True),
        StructField("current_year", IntegerType(), True)
    ])

    assert actual_df.schema == expected_schema
