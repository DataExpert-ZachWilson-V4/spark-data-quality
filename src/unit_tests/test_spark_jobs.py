from pyspark.sql.types import *
from collections import namedtuple
from chispa.dataframe_comparer import  assert_df_equality
from ..jobs import job_1

def test_actors_cumulative_scd(spark_session):
    actor_cols = ["actor", "actor_id", "films", "quality_class", "is_active", "current_year"]
    Actors = namedtuple('Actors', actor_cols)
    film_cols = ["film", "votes", "rating", "film_id"]
    Films = namedtuple("Films", film_cols)
    actor_films_cols = ["actor", "actor_id", "film", "votes", "rating", "film_id", "year"]
    ActorFilms = namedtuple('ActorFilms', actor_films_cols)

    actor_schema = StructType([
        StructField("actor", StringType()),
        StructField("actor_id", StringType()),
        StructField("films", ArrayType(
            StructType([
                    StructField("film", StringType()),
                    StructField("votes", IntegerType()),
                    StructField("rating", DoubleType()),
                    StructField("film_id", StringType())
                ])
            )
        ),
        StructField("quality_class", StringType()),
        StructField("is_active", BooleanType(), False),
        StructField("current_year", IntegerType())
    ])

    actor_films_schema = StructType([
        StructField("actor", StringType()),
        StructField("actor_id", StringType()),
        StructField("film", StringType()),
        StructField("votes", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("film_id", StringType()),
        StructField("year", IntegerType())
    ])


    actual_actors_data = [
        Actors(
            actor = "Harold Lloyd",
            actor_id = "nm0516001",
            films = [
                Films(
                    film = "The Patchwork Girl of Oz",
                    votes = 398,
                    rating = 5.5,
                    film_id = "tt0004457"
                )
            ],
            quality_class = "bad",
            is_active = False,
            current_year = 1915
        ),
        Actors(
            actor = "Lillian Gish",
            actor_id = "nm0001273",
            films = [
                Films(
                    film = "The Birth of a Nation",
                    votes = 22989,
                    rating = 6.3,
                    film_id = "tt0004972"
                ), Films(
                    film = "Judith of Bethulia",
                    votes = 1259,
                    rating = 6.1,
                    film_id = "tt0004181"
                ), Films(
                    "Home, Sweet Home",
                    190,
                    5.8,
                    "tt0003167"
                )
            ],
            quality_class = "average",
            is_active = True,
            current_year = 1915
        ),
    ]

    actual_actor_films_data = [
        ActorFilms(
            actor = "Lillian Gish",
            actor_id = "nm0001273",
            film = "Intolerance: Love's Struggle Throughout the Ages",
            votes = 14378,
            rating = 7.7,
            film_id = "tt0006864",
            year = 1916
        )
    ]

    expected_data = [
        Actors(
            actor = "Harold Lloyd",
            actor_id = "nm0516001",
            films = [
                Films(
                    film = "The Patchwork Girl of Oz",
                    votes = 398,
                    rating = 5.5,
                    film_id = "tt0004457"
                )
            ],
            quality_class = "bad",
            is_active = False,
            current_year = 1916
        ),
        Actors(
            actor = "Lillian Gish",
            actor_id = "nm0001273",
            films = [
                Films(
                    film = "Intolerance: Love's Struggle Throughout the Ages",
                    votes = 14378,
                    rating = 7.7,
                    film_id = "tt0006864"
                ), Films(
                    film = "The Birth of a Nation",
                    votes = 22989,
                    rating = 6.3,
                    film_id = "tt0004972"
                ), Films(
                    film = "Judith of Bethulia",
                    votes = 1259,
                    rating = 6.1,
                    film_id = "tt0004181"
                ), Films(
                    film = "Home, Sweet Home",
                    votes = 190,
                    rating = 5.8,
                    film_id = "tt0003167"
                )
            ],
            quality_class = "good",
            is_active = True,
            current_year = 1916
        ),
    ]
    spark = spark_session()
    actors_df = spark.createDataFrame(actual_actors_data, schema=actor_schema)
    actor_films_df = spark.createDataFrame(actual_actor_films_data, schema=actor_films_schema)

    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    expected_df = spark.createDataFrame(expected_data, actor_schema)

    actual_df = job_1.job_1(
        spark, 
        "actors",
        "actor_films",
        1916
    )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)