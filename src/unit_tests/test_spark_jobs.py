from collections import namedtuple

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

from jobs.job_1 import job_1
from jobs.job_2 import job_2


def test_job_1(spark: SparkSession):

    actors = namedtuple(
        "actors", "actor actor_id quality_class films is_active current_year"
    )
    actor_films = namedtuple(
        "actor_films", "actor actor_id film film_id year votes rating"
    )

    input_actor_films_data = [
        actor_films(
            actor="Brad Pitt",
            actor_id="1",
            film="The Big Short",
            film_id="11",
            year=2015,
            votes=255,
            rating=7.8,
        ),
        actor_films(
            actor="Brad Pitt",
            actor_id="1",
            film="By The Sea",
            film_id="22",
            year=2015,
            votes=235,
            rating=5.3,
        ),
        actor_films(
            actor="Chris Evans",
            actor_id="2",
            film="Ant-Man",
            film_id="33",
            year=2015,
            votes=555,
            rating=7.3,
        ),
        actor_films(
            actor="Chris Evans",
            actor_id="2",
            film="Avengers:Age of Ultron",
            film_id="44",
            year=2015,
            votes=885,
            rating=7.3,
        ),
    ]

    input_actors_data = [
        actors(
            actor="Brad Pitt",
            actor_id="1",
            quality_class="good",
            films=[
                ["Fury", "55", 2014, 754, 7.6],
            ],
            is_active=True,
            current_year=2014,
        ),
        actors(
            actor="Tom Hanks",
            actor_id="3",
            quality_class="good",
            films=[
                ["Sully", "525", 2014, 494, 7.4],
            ],
            is_active=True,
            current_year=2014,
        ),
    ]

    expected_actors_data = [
        actors(
            actor="Tom Hanks",
            actor_id="3",
            quality_class="good",
            films=[
                ["Sully", "525", 2014, 494, 7.4],
            ],
            is_active=False,
            current_year=2015,
        ),
        actors(
            actor="Brad Pitt",
            actor_id="1",
            quality_class="average",
            films=[
                ["Fury", "55", 2014, 754, 7.6],
                ["The Big Short", "11", 2015, 255, 7.8],
                ["By The Sea", "22", 2015, 235, 5.3],
            ],
            is_active=True,
            current_year=2015,
        ),
        actors(
            actor="Chris Evans",
            actor_id="2",
            quality_class="good",
            films=[
                ["Ant-Man", "33", 2015, 555, 7.3],
                ["Avengers:Age of Ultron", "44", 2015, 885, 7.3],
            ],
            is_active=True,
            current_year=2015,
        ),
    ]

    input_actor_films_df = spark.createDataFrame(input_actor_films_data)
    input_actor_films_df.createOrReplaceTempView("actor_films")

    input_actors_df = spark.createDataFrame(input_actors_data)
    input_actors_df.createOrReplaceTempView("actors")

    excepted_output_df = spark.createDataFrame(expected_actors_data)

    actual_output_df = job_1(spark, "actors")

    assert_df_equality(
        actual_output_df,
        excepted_output_df,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_job_2(spark: SparkSession):
    nba_game_details = namedtuple(
        "nba_game_details", "game_id team_id player_id"
    )

    input_data = [
        nba_game_details(game_id=1, team_id=1, player_id=1),
        nba_game_details(game_id=1, team_id=1, player_id=1),
        nba_game_details(game_id=1, team_id=1, player_id=2),
        nba_game_details(game_id=1, team_id=2, player_id=1),
        nba_game_details(game_id=1, team_id=2, player_id=1),
        nba_game_details(game_id=2, team_id=1, player_id=1),
        nba_game_details(game_id=2, team_id=1, player_id=1),
    ]

    expected_data = [
        nba_game_details(game_id=1, team_id=1, player_id=1),
        nba_game_details(game_id=1, team_id=1, player_id=2),
        nba_game_details(game_id=1, team_id=2, player_id=1),
        nba_game_details(game_id=2, team_id=1, player_id=1),
    ]

    input_df = spark.createDataFrame(input_data)
    input_df.createOrReplaceTempView("nba_game_details")

    excepted_output_df = spark.createDataFrame(expected_data)

    actual_output_df = job_2(spark, "nba_game_details")

    assert_df_equality(
        actual_output_df,
        excepted_output_df,
        ignore_nullable=True,
        ignore_row_order=True,
    )
