from chispa.dataframe_comparer import assert_df_equality

from chispa.dataframe_comparer import *

from collections import namedtuple

from jobs import job_2, job_1

# define named tuples
PlayerSeason = namedtuple("PlayerSeason", "player_name is_active current_season")
PlayerScd = namedtuple("PlayerScd", "player_name is_active start_season end_season")


def test_job_1(spark_session):
    input_table_name: str = "nba_players"
    source_data = [
        PlayerSeason("Michael Jordan", True, 2001),
        PlayerSeason("Michael Jordan", True, 2002),
        PlayerSeason("Michael Jordan", True, 2003),
        PlayerSeason("Michael Jordan", False, 2004),
        PlayerSeason("Michael Jordan", False, 2005),
        PlayerSeason("Scottie Pippen", True, 2001),
        PlayerSeason("Scottie Pippen", False, 2002),
        PlayerSeason("Scottie Pippen", False, 2003),
        PlayerSeason("Scottie Pippen", True, 2004),
        PlayerSeason("Scottie Pippen", True, 2005),
        PlayerSeason("LeBron James", True, 2003),
        PlayerSeason("LeBron James", True, 2004),
        PlayerSeason("LeBron James", True, 2005)
    ]
    source_df = spark_session.createDataFrame(source_data)

    from jobs.job_1 import job_1
    actual_df = job_1(spark_session, source_df, input_table_name)
    expected_data = [
        PlayerScd("Michael Jordan", True, 2001, 2003),
        PlayerScd("Michael Jordan", False, 2004, 2005),
        PlayerScd("Scottie Pippen", True, 2001, 2001),
        PlayerScd("Scottie Pippen", False, 2002, 2003),
        PlayerScd("Scottie Pippen", True, 2004, 2005),
        PlayerScd("LeBron James", True, 2003, 2005)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df.sort("player_name", "start_season"), expected_df.sort("player_name", "start_season"))

    # define named tuples


actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
actors = namedtuple("Actors", "actor actor_id films quality_class is_active current_year")


def test_job_2(spark_session):
    input_data = [
        actor_films("Wesley Snipes", "nm0000648", "The Art of War", "2000", "28821", "5.7", "tt0160009"),
        actor_films("Sean Connery", "nm0000125", "Finding Forrester", "2000", "83472", "7.3", "tt0181536"),
        actor_films("Kevin Bacon", "nm0000102", "Hollow Man", "2000", "124335", "5.8", "tt0164052"),
    ]

    # expected output based on our input
    expected_output = [
        actors("Wesley Snipes", "nm0000648", [["The Art of War", 28821, 5.7, "tt0160009", 2000]], "bad", 1, 2000),
        actors("Sean Connery", "nm0000125", [["Finding Forrester", 83472, 7.3, "tt0181536", 2000]], "good", 1, 2000),
        actors("Kevin Bacon", "nm0000102", [["Hollow Man", 124355, 5.8, "tt0164052", 2000]], "average", 1, 2000)
    ]

    input_data_df = spark_session.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("actor_films")

    expected_output_df = spark_session.createDataFrame(expected_output)

    # running the job
    actual_df = job_2(spark_session, "actors")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
