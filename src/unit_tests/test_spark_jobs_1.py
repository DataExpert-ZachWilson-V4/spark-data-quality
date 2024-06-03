from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from collections import namedtuple
from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    ArrayType,
    BooleanType,
)

# Define the schema for the films array

actor_schema = StructType(
    [
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField(
            "films",
            ArrayType(
                StructType(
                    [
                        StructField("film", StringType(), True),
                        StructField("year", IntegerType(), True),
                        StructField("votes", IntegerType(), True),
                        StructField("rating", DoubleType(), True),
                        StructField("film_id", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("average_rating", DoubleType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", LongType(), True),
    ]
)

films_schema = StructType(
    [
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("film", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("film_id", StringType(), True),
    ]
)

Actor = namedtuple(
    "Actor", "actor actor_id films average_rating quality_class is_active current_year"
)

ActorFilms = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")

input_actor_films_data = [
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Jayne Mansfield's Car",
        year=2012,
        votes=2945,
        rating=6.3,
        film_id="tt1781840",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="X-Men: First Class",
        year=2011,
        votes=650154,
        rating=7.7,
        film_id="tt1270798",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Crazy, Stupid, Love.",
        year=2011,
        votes=478740,
        rating=7.4,
        film_id="tt1570728",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Elephant White",
        year=2011,
        votes=10447,
        rating=5.1,
        film_id="tt1578882",
    ),
    ActorFilms(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="John Dies at the End",
        year=2012,
        votes=36363,
        rating=6.4,
        film_id="tt1783732",
    ),
    ActorFilms(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="At Any Price",
        year=2012,
        votes=7282,
        rating=5.6,
        film_id="tt1937449",
    ),
    ActorFilms(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Hellbenders",
        year=2012,
        votes=1401,
        rating=4.8,
        film_id="tt1865393",
    ),
    ActorFilms(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Green Lantern",
        year=2011,
        votes=268721,
        rating=5.5,
        film_id="tt1133985",
    ),
    ActorFilms(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Cowboys & Aliens",
        year=2011,
        votes=208961,
        rating=6.0,
        film_id="tt0409847",
    ),
]

input_actors_data = [
    Actor(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        films=[
            {
                "film": "Jayne Mansfield's Car",
                "year": 2012,
                "votes": 2945,
                "rating": 6.3,
                "film_id": "tt1781840",
            },
            {
                "film": "X-Men: First Class",
                "year": 2011,
                "votes": 650154,
                "rating": 7.7,
                "film_id": "tt1270798",
            },
            {
                "film": "Crazy, Stupid, Love.",
                "year": 2011,
                "votes": 478740,
                "rating": 7.4,
                "film_id": "tt1570728",
            },
            {
                "film": "Elephant White",
                "year": 2011,
                "votes": 10447,
                "rating": 5.1,
                "film_id": "tt1578882",
            },
        ],
        average_rating=6.3,
        quality_class="average",
        is_active=True,
        current_year=2012,
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        films=[
            {
                "film": "John Dies at the End",
                "year": 2012,
                "votes": 36363,
                "rating": 6.4,
                "film_id": "tt1783732",
            },
            {
                "film": "At Any Price",
                "year": 2012,
                "votes": 7282,
                "rating": 5.6,
                "film_id": "tt1937449",
            },
            {
                "film": "Hellbenders",
                "year": 2012,
                "votes": 1401,
                "rating": 4.8,
                "film_id": "tt1865393",
            },
            {
                "film": "Green Lantern",
                "year": 2011,
                "votes": 268721,
                "rating": 5.5,
                "film_id": "tt1133985",
            },
            {
                "film": "Cowboys & Aliens",
                "year": 2011,
                "votes": 208961,
                "rating": 6.0,
                "film_id": "tt0409847",
            },
        ],
        average_rating=5.60,
        quality_class="bad",
        is_active=True,
        current_year=2012,
    ),
]


# Define the schema for the films array
def test_spark_queries_1(spark_session):

    input_actor_films_dataframe = spark_session.createDataFrame(input_actor_films_data, films_schema)
    input_actors_dataframe = spark_session.createDataFrame(
        input_actors_data, actor_schema
    )

    actual_dataframe = job_1(
        spark_session, input_actor_films_dataframe, input_actors_dataframe
    )

    expected_output = input_actors_data
    expected_df = spark_session.createDataFrame(expected_output)
    assert_df_equality(actual_dataframe, expected_df, ignore_nullable=True)
